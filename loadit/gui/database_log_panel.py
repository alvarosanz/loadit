import wx
import logging


log = logging.getLogger()


class DatabaseLogPanel(wx.Panel):

    def __init__(self, parent, root):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        self._log = DatabaseLogTextCtrl(self, style=wx.TE_MULTILINE + wx.TE_READONLY)
        sizer.Add(self._log, 1, wx.ALL + wx.EXPAND, 5)
        self.log = logging.StreamHandler(self._log)
        self.log.setLevel(logging.INFO)
        self.log.setFormatter(logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S'))

        # Buttons
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        clear_button = wx.Button(self, id=wx.ID_ANY, label='Clear')
        clear_button.Bind(wx.EVT_BUTTON, self.clear)
        field_sizer.Add(clear_button, 0, wx.LEFT + wx.EXPAND, 5)
        save_button = wx.Button(self, id=wx.ID_ANY, label='Save As')
        save_button.Bind(wx.EVT_BUTTON, self.save)
        field_sizer.Add(save_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)

    def clear(self, event):
        self._log.SetValue('')

    def save(self, event):
        wildcard = 'LOG files (*.log)|*.log'

        with wx.FileDialog(self.root, 'Save log file', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT, wildcard=wildcard) as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:

                try:

                    with open(dialog.GetPath(), 'w') as f:
                        f.write(self._log.GetValue())

                    log.info('Log saved')
                except Exception as e:
                    log.error(str(e))


class DatabaseLogTextCtrl(wx.TextCtrl):

    def write(self, msg):
        super().AppendText(msg)
        wx.Yield()
