import json
import wx
import wx.grid
import pyarrow as pa


class RecordBatchTable(wx.grid.GridTableBase):

    def __init__(self, record_batch):
        super().__init__()
        self.record_batch = record_batch

    def GetNumberRows(self):
        return self.record_batch.num_rows

    def GetNumberCols(self):
        return self.record_batch.num_columns

    def IsEmptyCell(self, row, col):
        return False

    def GetValue(self, row, col):
        return self.record_batch.column(col)[row].as_py()

    def GetColLabelValue(self, col):
        return self.record_batch.schema[col].name


class RecordBatchTableGrid(wx.grid.Grid):

    def __init__(self, parent):
        super().__init__(parent)
        self.HideRowLabels()
        self.DisableDragGridSize()
        self.DisableDragRowSize()

    def update(self, record_batch):
        self.SetTable(RecordBatchTable(record_batch), True)
        index_columns = json.loads(record_batch.schema.metadata[b'index_columns'].decode())

        for i, field in enumerate(record_batch.schema):
            column_attribute = wx.grid.GridCellAttr()
            column_attribute.SetReadOnly()

            if field.name == 'Group':
                self.SetColSize(i, 200)
                self.SetColMinimalWidth(i, 200)
            else:
                self.SetColSize(i, 80)
                self.SetColMinimalWidth(i, 80)

            self.AutoSizeColLabelSize(i)

            if pa.types.is_integer(field.type):
                self.SetColFormatNumber(i)
            elif pa.types.is_floating(field.type):
                column_attribute.SetRenderer(wx.grid.GridCellFloatRenderer(width=-1, precision=4,
                                             format=wx.grid.GRID_FLOAT_FORMAT_COMPACT))

            if field.name in index_columns:
                column_attribute.SetBackgroundColour(wx.Colour(235, 235, 235))

            self.SetColAttr(i, column_attribute)
