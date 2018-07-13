import wx


class DatabaseTab(wx.Panel):

    def __init__(self, parent, database, is_local=True):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.parent = parent
        self.database = database
        self.is_local = is_local