import wx
import logging


class DatabaseLogPanel(wx.Panel):

    def __init__(self, parent):
        super().__init__(parent=parent, id=wx.ID_ANY)

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
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)

    def clear(self, event):
        self._log.SetValue('')


class DatabaseLogTextCtrl(wx.TextCtrl):

    def write(self, msg):
        super().AppendText(msg)
        wx.Yield()
