import wx
from loadit.server import SERVER_PORT


class ConnectDialog(wx.Dialog):

    def __init__(self, parent, address=None):
        super().__init__(parent)
        self.SetTitle('Connect')
        self.SetSize((300, 200))
        self._ip = wx.TextCtrl(self, value=address.split(':')[0] if address else '', size=(125, -1))
        self._port = wx.TextCtrl(self, value=address.split(':')[1] if address else str(SERVER_PORT), size=(20, -1))
        self._user = wx.TextCtrl(self)
        self._password = wx.TextCtrl(self, style=wx.TE_PASSWORD)
        self.InitUI()

    def InitUI(self):
        sizer = wx.BoxSizer(wx.VERTICAL)

        sizer.Add(-1, 8)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='IP:', style=wx.ALIGN_RIGHT), 0, wx.ALL, 5)
        field_sizer.Add(self._ip, 1, wx.ALL + wx.EXPAND, 5)
        field_sizer.Add(8, -1)
        field_sizer.Add(wx.StaticText(self, label='Port:', style=wx.ALIGN_RIGHT), 0, wx.ALL, 5)
        field_sizer.Add(self._port, 1, wx.ALL + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.EXPAND, 15)

        sizer.Add(-1, 16)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='User:', size=(70, -1), style=wx.ALIGN_RIGHT), 0, wx.ALL, 5)
        field_sizer.Add(self._user, 1, wx.ALL + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.EXPAND, 15)

        sizer.Add(-1, 8)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Password:', size=(70, -1), style=wx.ALIGN_RIGHT), 0, wx.ALL, 5)
        field_sizer.Add(self._password, 1, wx.ALL + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.EXPAND, 15)

        sizer.Add(-1, 8)
        sizer.Add(self.CreateButtonSizer(wx.OK + wx.CANCEL), 0, wx.EXPAND + wx.ALIGN_BOTTOM, 5)
        
        self.SetSizer(sizer)


    @property
    def address(self):
        return f"{self._ip.Value}:{self._port.Value}"

    @property
    def user(self):
        return self._user.Value

    @property
    def password(self):
        return self._password.Value