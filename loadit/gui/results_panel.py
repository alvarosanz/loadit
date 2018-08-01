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
        self.save_as_csv_button = wx.Button(self, id=wx.ID_ANY, label='Save as .csv')
        self.save_as_csv_button.Bind(wx.EVT_BUTTON, self.save_as_csv)
        field_sizer.Add(self.save_as_csv_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.save_as_parquet_button = wx.Button(self, id=wx.ID_ANY, label='Save as .parquet')
        self.save_as_parquet_button.Bind(wx.EVT_BUTTON, self.save_as_parquet)
        field_sizer.Add(self.save_as_parquet_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)

    def update(self, record_batch):
        self.results = record_batch
        self._results.update(record_batch)

    def save_as_csv(self, event):
        self.save('csv')

    def save_as_parquet(self, event):
        self.save('parquet')

    def save(self, output_type):

        with wx.FileDialog(self.root, 'Save output file', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT, wildcard=f'{output_type.upper()} files (*.{output_type})|*.{output_type}') as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                write_query(self.results, dialog.GetPath())
                self.root.statusbar.SetStatusText('Results saved')

    def copy_to_clipboard(self, event):
        self.root.statusbar.SetStatusText('Copying results to clipboard ...')
        get_dataframe(self.results, False).to_clipboard()
        self.root.statusbar.SetStatusText('Results copied to clipboard')
