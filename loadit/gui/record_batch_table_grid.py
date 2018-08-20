import json
import wx
import wx.grid
import pyarrow as pa


class RecordBatchTable(wx.grid.GridTableBase):

    def __init__(self, record_batch):
        super().__init__()
        self.record_batch = record_batch
        self.index_names = json.loads(record_batch.schema.metadata[b'index_names'])
        self.index = json.loads(record_batch.schema.metadata[b'index'])
        self.nindex = len(self.index)

        if record_batch.schema.metadata[b'sorted_by'] == b'0':
            self.sort_by_LID = True
        else:
            self.sort_by_LID = False

    def GetNumberRows(self):
        return self.record_batch.num_rows

    def GetNumberCols(self):
        return self.record_batch.num_columns + self.nindex

    def IsEmptyCell(self, row, col):
        return False

    def GetValue(self, row, col):

        if col < self.nindex:

            if self.nindex == 1:
                return self.index[col][row % len(self.index[col])]
            else:

                if self.sort_by_LID:

                    if col == 0:
                        return self.index[0][row // len(self.index[1])]
                    else:
                        return self.index[1][row % len(self.index[1])]
                    
                else:

                    if col == 0:
                        return self.index[0][row % len(self.index[0])]
                    else:
                        return self.index[1][row // len(self.index[0])]
        
        else:
            return self.record_batch.column(col - self.nindex)[row].as_py()

    def GetColLabelValue(self, col):

        if col < self.nindex:
            return self.index_names[col]
        else:
            return self.record_batch.schema[col - self.nindex].name


class RecordBatchTableGrid(wx.grid.Grid):

    def __init__(self, parent):
        super().__init__(parent)
        self.HideRowLabels()
        self.DisableDragGridSize()
        self.DisableDragRowSize()

    def update(self, record_batch):
        self.SetTable(RecordBatchTable(record_batch), True)
        index_names = json.loads(record_batch.schema.metadata[b'index_names'])

        for i, field in enumerate(index_names + list(record_batch.schema)):
            column_attribute = wx.grid.GridCellAttr()
            column_attribute.SetReadOnly()
            self.SetColSize(i, 80)
            self.SetColMinimalWidth(i, 80)

            if i < len(index_names):
                column_attribute.SetBackgroundColour(wx.Colour(235, 235, 235))

                if field == 'Group':
                    self.SetColSize(i, 200)
                    self.SetColMinimalWidth(i, 200)

            self.AutoSizeColLabelSize(i)

            if i < len(index_names) and field != 'Group' or i >= len(index_names) and pa.types.is_integer(field.type):
                self.SetColFormatNumber(i)
            elif i >= len(index_names) and pa.types.is_floating(field.type):
                column_attribute.SetRenderer(wx.grid.GridCellFloatRenderer(width=-1, precision=4,
                                             format=wx.grid.GRID_FLOAT_FORMAT_COMPACT))

            self.SetColAttr(i, column_attribute)
