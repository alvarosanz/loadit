import wx
from loadit.gui.record_batch_table_grid import RecordBatchTableGrid
from loadit.database import write_query, get_dataframe


class ResultsPanel(wx.Panel):

    def __init__(self, parent, root):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.results = None

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        self._results = RecordBatchTableGrid(self)
        self._results.SetReadOnly(5,5, True)
        sizer.Add(self._results, 1, wx.ALL + wx.EXPAND, 5)

        # Buttons
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.copy_to_clipboard_button = wx.Button(self, id=wx.ID_ANY, label='Copy to clipboard')
        self.copy_to_clipboard_button.Bind(wx.EVT_BUTTON, self.copy_to_clipboard)
        field_sizer.Add(self.copy_to_clipboard_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.save_button = wx.Button(self, id=wx.ID_ANY, label='Save As')
        self.save_button.Bind(wx.EVT_BUTTON, self.save)
        field_sizer.Add(self.save_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)

    def update(self, record_batch):
        self.results = record_batch
        self._results.update(record_batch)

    def save(self, event):
        wildcard = 'CSV files (*.csv)|*.csv|EXCEL files (*.xlsx)|*.xlsx|PARQUET files (*.parquet)|*.parquet'

        with wx.FileDialog(self.root, 'Save output file', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT, wildcard=wildcard) as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                write_query(self.results, dialog.GetPath())
                self.root.statusbar.SetStatusText('Results saved')

    def copy_to_clipboard(self, event):
        self.root.statusbar.SetStatusText('Copying results to clipboard ...')
        get_dataframe(self.results, False).to_clipboard()
        self.root.statusbar.SetStatusText('Results copied to clipboard')
