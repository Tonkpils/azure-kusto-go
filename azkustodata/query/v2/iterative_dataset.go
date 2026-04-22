package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/Azure/azure-kusto-go/azkustodata/errors"
	"github.com/Azure/azure-kusto-go/azkustodata/query"
)

// DefaultIoCapacity is the default capacity of the channel that receives frames from the Kusto service. Lower capacity means less memory usage, but might cause the channel to block if the frames are not consumed fast enough.
const DefaultIoCapacity = 1

const DefaultRowCapacity = 1000

const DefaultTableCapacity = 1

const PrimaryResultTableKind = "PrimaryResult"

// iterativeDataset contains the main logic of parsing a v2 dataset.
// v2 is made from a series of frames, which are decoded by turn.
type iterativeDataset struct {
	query.BaseDataset

	// results is a channel that sends the parsed results as they are decoded.
	results chan query.TableResult

	// rowCapacity is the amount of rows to buffer per table.
	rowCapacity int

	// cancel is a function to cancel the reading of the dataset, and is called when the dataset is closed.
	cancel context.CancelFunc

	// currentTable is the table that is currently being read, as it can contain multiple fragments.
	currentTable *iterativeTable

	// queryProperties is a table that contains the query properties, and is sent after the primary results.
	queryProperties query.IterativeTable

	// jsonData is a channel that receives the raw JSON data from the Kusto service.
	jsonData chan interface{}
}

// NewIterativeDataset creates a new IterativeDataset from a ReadCloser.
// ioCapacity is the amount of buffered rows to keep in memory.
// tableCapacity is the amount of tables to buffer.
// rowCapacity is the amount of rows to buffer per table.
func NewIterativeDataset(ctx context.Context, r io.ReadCloser, ioCapacity int, rowCapacity int, tableCapacity int) (query.IterativeDataset, error) {

	ctx, cancel := context.WithCancel(ctx)

	d := &iterativeDataset{
		BaseDataset:     query.NewBaseDataset(ctx, errors.OpQuery, PrimaryResultTableKind),
		results:         make(chan query.TableResult, tableCapacity),
		rowCapacity:     rowCapacity,
		cancel:          cancel,
		currentTable:    nil,
		queryProperties: nil,
		jsonData:        make(chan interface{}, ioCapacity),
	}

	// This ctor will fail if we get a non-json response
	// In this case, we want to return it immediately
	reader, err := newFrameReader(r, ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	// Spin up two goroutines - one to parse the dataset, and one to read the frames.
	go parseRoutine(d, cancel)
	go readRoutine(reader, d)

	return d, nil
}

// readRoutine reads the frames from the Kusto service and sends them to the buffered channel.
// This is so we could keep up if the IO is faster than the consumption of the frames.
func readRoutine(reader *frameReader, d *iterativeDataset) {
	loop := true

	for loop {
		line, err := reader.advance()
		if err != nil {
			if err != io.EOF {
				select {
				case <-d.Context().Done():
				// When we send data, we always make sure that the context isn't cancelled, so we don't block forever.
				case d.jsonData <- err:
				}
			}
			loop = false
		} else {
			select {
			case <-d.Context().Done():
				loop = false
			case d.jsonData <- line:
			}
		}
	}

	if err := reader.close(); err != nil {
		select {
		case <-d.Context().Done():
		case d.jsonData <- err:
		}
	}

	close(d.jsonData)
}

// parseRoutine reads the frames from the buffered channel and parses them.
func parseRoutine(d *iterativeDataset, cancel context.CancelFunc) {

	err := readDataSet(d)
	if err != nil {
		select {
		case d.results <- query.TableResultError(err):
		case <-d.Context().Done():
		}
		cancel()
	}

	if d.currentTable != nil {
		d.currentTable.finishTable([]OneApiError{}, err)
	}

	cancel()
	close(d.results)
}

func readDataSet(d *iterativeDataset) error {

	var err error

	// The first frame should be a DataSetHeader.
	if headerDec, _, err := nextFrame(d); err == nil {
		if _, parseErr := parseDataSetHeader(headerDec); parseErr != nil {
			return parseErr
		}
	} else {
		return err
	}

	// Next up, we expect the QueryProperties table.
	// In fragmented mode this is a DataTable; in progressive mode it may
	// arrive as a TableHeader → TableFragment* → TableCompletion sequence.
	if decoder, frameType, err := nextFrame(d); err == nil {
		switch frameType {
		case DataTableFrameType:
			if err = handleDataTable(d, decoder); err != nil {
				return err
			}
		case TableHeaderFrameType:
			if err = readSecondaryTable(d, decoder); err != nil {
				return err
			}
		default:
			return errors.ES(errors.OpQuery, errors.KInternal,
				"unexpected frame type %s for QueryProperties, expected DataTable or TableHeader", frameType)
		}
	} else {
		return err
	}

	// We then iterate over the remaining tables.
	// If we get a TableHeader, we peek its TableKind to route it:
	//   - PrimaryResult → readPrimaryTable (streamed to user)
	//   - anything else → readSecondaryTable (buffered internally)
	// If we get a DataTable, it's a secondary table in fragmented mode.
	// If we get a DataSetCompletion, we are done.
	for decoder, frameType, err := nextFrame(d); err == nil; decoder, frameType, err = nextFrame(d) {
		if frameType == DataTableFrameType {
			if err = handleDataTable(d, decoder); err != nil {
				return err
			}
			continue
		}

		if frameType == TableHeaderFrameType {
			// Decode the header to check if this is a primary result or a
			// secondary table (QueryCompletionInformation, etc.).
			header := TableHeader{}
			if err = decoder.Decode(&header); err != nil {
				return err
			}

			if header.TableKind == PrimaryResultTableKind {
				if err = handleTableHeader(d, header); err != nil {
					return err
				}
				if err = readPrimaryTableFragments(d, header); err != nil {
					return err
				}
			} else {
				if err = readSecondaryTableFragments(d, header); err != nil {
					return err
				}
			}
			continue
		}

		if frameType == DataSetCompletionFrameType {
			err = readDataSetCompletion(decoder)
			if err != nil {
				return err
			}
			return nil
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s, expected DataTable, TableHeader, or DataSetCompletion", frameType)
	}

	return err
}

// nextFrame reads the next frame from the buffered channel.
// It doesn't parse the frame yet, but peeks the frame type to determine how to handle it.
func nextFrame(d *iterativeDataset) (*json.Decoder, FrameType, error) {
	var line []byte
	select {
	case <-d.Context().Done():
		return nil, "", errors.ES(errors.OpQuery, errors.KInternal, "context cancelled")
	case val := <-d.jsonData:
		if val == nil {
			return nil, "", errors.ES(errors.OpQuery, errors.KInternal, "nil value received from channel")
		}
		if err, ok := val.(error); ok {
			return nil, "", err
		}
		line = val.([]byte)
	}

	frameType, err := peekFrameType(line)
	if err != nil {
		return nil, "", err
	}

	return json.NewDecoder(bytes.NewReader(line)), frameType, nil
}

// readDataSetCompletion reads the DataSetCompletion frame, and returns any errors it might contain.
func readDataSetCompletion(dec *json.Decoder) error {
	completion := DataSetCompletion{}
	err := dec.Decode(&completion)
	if err != nil {
		return err
	}
	if completion.HasErrors {
		return combineOneApiErrors(completion.OneApiErrors)
	}
	return nil
}

// combineOneApiErrors combines multiple OneApiErrors into a single error, de-duping them if needed.
func combineOneApiErrors(errs []OneApiError) error {
	c := errors.NewCombinedError()
	for i := range errs {
		c.AddError(&errs[i])
	}
	return c.GetError()
}

// readPrimaryTable reads a primary table from the dataset.
// Decodes the header from dec, then reads fragments until TableCompletion.
func readPrimaryTable(d *iterativeDataset, dec *json.Decoder) error {
	header := TableHeader{}
	if err := dec.Decode(&header); err != nil {
		return err
	}

	if err := handleTableHeader(d, header); err != nil {
		return err
	}

	return readPrimaryTableFragments(d, header)
}

// readPrimaryTableFragments reads TableFragment/TableProgress/TableCompletion
// frames for an already-opened primary table.
func readPrimaryTableFragments(d *iterativeDataset, header TableHeader) error {
	for i := 0; ; {
		dec, frameType, err := nextFrame(d)
		if err != nil {
			return err
		}
		if frameType == TableFragmentFrameType {
			fragment := TableFragment{Columns: header.Columns, PreviousIndex: i}
			err = dec.Decode(&fragment)
			if err != nil {
				return err
			}
			if fragment.TableFragmentType == "DataReplace" {
				return errors.ES(errors.OpQuery, errors.KInternal,
					"DataReplace TableFragment is not supported by the streaming API")
			}
			i += len(fragment.Rows)
			if err = handleTableFragment(d, fragment); err != nil {
				return err
			}
			continue
		}

		if frameType == TableProgressFrameType {
			var progress TableProgress
			if err = dec.Decode(&progress); err != nil {
				return err
			}
			continue
		}

		if frameType == TableCompletionFrameType {
			completion := TableCompletion{}
			err = dec.Decode(&completion)
			if err != nil {
				return err
			}

			if err = handleTableCompletion(d, completion); err != nil {
				return err
			}

			break
		}

		return errors.ES(errors.OpQuery, errors.KInternal, "unexpected frame type %s, expected TableFragment, TableProgress, or TableCompletion", frameType)
	}

	return nil
}

// readSecondaryTable reads a non-primary table that arrives as a
// TableHeader → TableFragment* → TableCompletion sequence.
// This happens in progressive mode where secondary tables (QueryProperties,
// QueryCompletionInformation) use the same framing as primary tables.
func readSecondaryTable(d *iterativeDataset, dec *json.Decoder) error {
	header := TableHeader{}
	if err := dec.Decode(&header); err != nil {
		return err
	}

	return readSecondaryTableFragments(d, header)
}

// readSecondaryTableFragments buffers all rows from a secondary table's
// fragment sequence, then dispatches via processSecondaryDataTable.
func readSecondaryTableFragments(d *iterativeDataset, header TableHeader) error {
	var rows []query.Row
	for {
		dec, frameType, err := nextFrame(d)
		if err != nil {
			return err
		}

		if frameType == TableFragmentFrameType {
			fragment := TableFragment{Columns: header.Columns, PreviousIndex: len(rows)}
			if err = dec.Decode(&fragment); err != nil {
				return err
			}
			rows = append(rows, fragment.Rows...)
			continue
		}

		if frameType == TableProgressFrameType {
			var progress TableProgress
			if err = dec.Decode(&progress); err != nil {
				return err
			}
			continue
		}

		if frameType == TableCompletionFrameType {
			completion := TableCompletion{}
			if err = dec.Decode(&completion); err != nil {
				return err
			}
			if completion.OneApiErrors != nil {
				if combinedErr := combineOneApiErrors(completion.OneApiErrors); combinedErr != nil {
					return combinedErr
				}
			}
			break
		}

		return errors.ES(errors.OpQuery, errors.KInternal,
			"unexpected frame type %s in secondary table, expected TableFragment, TableProgress, or TableCompletion", frameType)
	}

	dt := DataTable{Header: header, Rows: rows}
	return processSecondaryDataTable(d, dt)
}

// handleDataTable reads a DataTable frame from the dataset, which aren't iterative.
// In Fragmented V2, these are only the metadata tables - QueryProperties and QueryCompletionInformation.
func handleDataTable(d *iterativeDataset, dec *json.Decoder) error {
	var dt DataTable
	if err := dec.Decode(&dt); err != nil {
		return err
	}

	return processSecondaryDataTable(d, dt)
}

// processSecondaryDataTable dispatches a decoded secondary table by its kind.
// Used by both handleDataTable (fragmented mode) and readSecondaryTable (progressive mode).
func processSecondaryDataTable(d *iterativeDataset, dt DataTable) error {
	if dt.Header.TableKind == PrimaryResultTableKind {
		return errors.ES(d.Op(), errors.KInternal, "received a DataTable frame for a primary result table")
	}
	switch dt.Header.TableKind {
	case QueryPropertiesKind:
		res, err := newTable(d, dt)
		if err != nil {
			return err
		}
		d.queryProperties = iterativeWrapper{res}
	case QueryCompletionInformationKind:
		if d.queryProperties != nil {
			d.sendTable(d.queryProperties)
		}

		res, err := newTable(d, dt)
		if err != nil {
			return err
		}
		d.sendTable(iterativeWrapper{res})

	default:
		return errors.ES(d.Op(), errors.KInternal, "unknown secondary table - %s %s", dt.Header.TableName, dt.Header.TableKind)
	}

	return nil
}

func handleTableCompletion(d *iterativeDataset, tc TableCompletion) error {
	if d.currentTable == nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame while no streaming table was open")
	}
	if int(d.currentTable.Index()) != tc.TableId {
		return errors.ES(d.Op(), errors.KInternal, "received a TableCompletion frame for table %d while table %d was open", tc.TableId, int((d.currentTable).Index()))
	}

	d.currentTable.finishTable(tc.OneApiErrors, nil)

	d.currentTable = nil

	return nil
}

func handleTableFragment(d *iterativeDataset, tf TableFragment) error {
	if d.currentTable == nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableFragment frame while no streaming table was open")
	}

	d.currentTable.addRawRows(tf.Rows)

	return nil
}

func handleTableHeader(d *iterativeDataset, th TableHeader) error {
	if d.currentTable != nil {
		return errors.ES(d.Op(), errors.KInternal, "received a TableHeader frame while a streaming table was still open")
	}

	// Read the table header, set it as the current table, and send it to the user (so they can start reading rows)

	t, err := NewIterativeTable(d, th)
	if err != nil {
		return err
	}

	d.currentTable = t.(*iterativeTable)
	d.sendTable(d.currentTable)

	return nil
}

// sendTable sends a table to results channel for the user, or cancels if the context is done.
func (d *iterativeDataset) sendTable(tb query.IterativeTable) {
	select {
	case <-d.Context().Done():
		return
	case d.results <- query.TableResultSuccess(tb):
		return
	}
}

// Tables returns a channel that sends the tables as they are parsed.
func (d *iterativeDataset) Tables() <-chan query.TableResult {
	return d.results
}

// Close closes the dataset, cancelling the context and closing the results channel.
func (d *iterativeDataset) Close() error {
	d.cancel()
	return nil
}

// ToDataset reads the entire iterative dataset, converting it to a regular dataset.
func (d *iterativeDataset) ToDataset() (query.Dataset, error) {
	tables := make([]query.Table, 0, len(d.results))

	defer d.Close()

	for tb := range d.Tables() {
		if tb.Err() != nil {
			return nil, tb.Err()
		}

		table, err := tb.Table().ToTable()
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return query.NewDataset(d, tables), nil
}
