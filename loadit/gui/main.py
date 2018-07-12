import os
import sys
import threading
import wx
import wx.aui
from loadit.client import Client
from loadit.misc import humansize


class MainWindow(wx.Frame):

    def __init__(self, parent, title):
        super().__init__(parent, title=title, size=(1024, 640))
        self.client = Client()

        # Statusbar
        self.statusbar = CustomStatusBar(self)
        self.SetStatusBar(self.statusbar)
        sys.stdout = self.statusbar # Redirect stdout to statusbar

        # Menubar
        self.menubar = wx.MenuBar()
        self.SetMenuBar(self.menubar)
        self.Bind(wx.EVT_MENU_OPEN, self.update_menubar, self.menubar)

        # File menu
        self.filemenu = wx.Menu()
        self.menubar.Append(self.filemenu, '&File')
        self.filemenu_new = self.filemenu.Append(wx.ID_ANY, 'New', 'Create a new database')
        self.filemenu_open= self.filemenu.Append(wx.ID_ANY, 'Open', 'Open an existing database')
        self.filemenu.AppendSeparator()
        self.filemenu_close= self.filemenu.Append(wx.ID_ANY, 'Close', 'Close current database')
        self.filemenu.AppendSeparator()
        self.filemenu_exit = self.filemenu.Append(wx.ID_EXIT, 'Exit', 'Exit the program')
        self.Bind(wx.EVT_MENU, self.on_new, self.filemenu_new)
        self.Bind(wx.EVT_MENU, self.on_open, self.filemenu_open)
        self.Bind(wx.EVT_MENU, self.on_close, self.filemenu_close)

        # Remote menu
        self.remotemenu = wx.Menu()
        self.menubar.Append(self.remotemenu, '&Remote')
        self.remotemenu_connect = self.remotemenu.Append(wx.ID_ANY, 'Connect', 'Connect to the remote server')
        self.remotemenu_disconnect = self.remotemenu.Append(wx.ID_ANY, 'Disconnect', 'Disconnect from the remote server')
        self.remotemenu.AppendSeparator()
        self.remotemenu_new = self.remotemenu.Append(wx.ID_ANY, 'New', 'Create a new remote database')
        self.remotemenu_open= self.remotemenu.Append(wx.ID_ANY, 'Open', 'Open an existing remote database')
        self.remotemenu.AppendSeparator()
        self.Bind(wx.EVT_MENU, self.on_connect, self.remotemenu_connect)
        self.Bind(wx.EVT_MENU, self.on_disconnect, self.remotemenu_disconnect)
        self.Bind(wx.EVT_MENU, self.on_new_remote, self.remotemenu_new)
        self.Bind(wx.EVT_MENU, self.on_open_remote, self.remotemenu_open)

        # Management menu
        self.managementmenu = wx.Menu()
        self.managementmenu_info = self.managementmenu.Append(wx.ID_ANY, 'Info', 'Show remote cluster info')
        self.managementmenu_sessions = self.managementmenu.Append(wx.ID_ANY, 'Sessions', 'Manage sessions')
        self.managementmenu_sync = self.managementmenu.Append(wx.ID_ANY, 'Sync', 'Sync remote databases')
        self.managementmenu_shutdown = self.managementmenu.Append(wx.ID_ANY, 'Shutdown', 'Shutdown the entire cluster')
        self.remotemenu_management = self.remotemenu.AppendSubMenu(self.managementmenu, 'Management', 'Manage remote cluster')
        self.Bind(wx.EVT_MENU, self.on_info, self.managementmenu_info)
        self.Bind(wx.EVT_MENU, self.on_sessions, self.managementmenu_sessions)
        self.Bind(wx.EVT_MENU, self.on_sync, self.managementmenu_sync)
        self.Bind(wx.EVT_MENU, self.on_shutdown, self.managementmenu_shutdown)

        # Database notebook
        self.remote_tabs = list()
        self.notebook = wx.aui.AuiNotebook(self)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.notebook, 1, wx.EXPAND)
        self.SetSizer(sizer)
        self.Bind(wx.aui.EVT_AUINOTEBOOK_PAGE_CLOSE, self.on_tab_close, self.notebook)

        self.Show()

    def update_menubar(self, event):
        # File Menu
        if self.notebook.GetPageCount():
            self.filemenu_close.Enable(True)
        else:
            self.filemenu_close.Enable(False)

        # Remote menu
        self.remotemenu_connect.Enable(True)
        self.remotemenu_disconnect.Enable(False)
        self.remotemenu_new.Enable(False)
        self.remotemenu_open.Enable(False)
        self.remotemenu_management.Enable(False)

        if self.client._authentication:
            session = self.client.session
            self.remotemenu_connect.Enable(False)
            self.remotemenu_disconnect.Enable(True)

            if session['create_allowed']:
                self.remotemenu_new.Enable(True)

            self.remotemenu_open.Enable(True)

            if session['is_admin']:
                self.remotemenu_management.Enable(True)

    def _new_tab(self, database, msg, is_local=True):

        try:
            tab = TabPanel(self.notebook, database, is_local)
            self.notebook.AddPage(tab, os.path.basename(database.path))
            self.notebook.SetSelection(self.notebook.GetPageIndex(tab))
            self.notebook.SetPageToolTip(self.notebook.GetPageIndex(tab), database.path)

            if not is_local:
                self.remote_tabs.append(tab)

            self.statusbar.SetStatusText(msg)
        except Exception as e:
            self.statusbar.SetStatusText(str(e))

    def on_new(self, event):
        dialog = wx.DirDialog(self, 'New database', '', style=wx.DD_DEFAULT_STYLE)

        if dialog.ShowModal() == wx.ID_OK:
            name_dialog = wx.TextEntryDialog(self, 'Name:','New database')

            if name_dialog.ShowModal() == wx.ID_OK:
                database = os.path.join(dialog.GetPath(), name_dialog.GetValue())
                self._new_tab(self.client.create_database(database), 'Database created', is_local=True)

            name_dialog.Destroy()

        dialog.Destroy()

    def on_open(self, event):
        dialog = wx.DirDialog(self, 'Open database', '', style=wx.DD_DEFAULT_STYLE)

        if dialog.ShowModal() == wx.ID_OK:
            database = dialog.GetPath()
            self._new_tab(self.client.load_database(database), 'Database loaded', is_local=True)

        dialog.Destroy()

    def on_close(self, event):
        tab = self.notebook.GetCurrentPage()
        self.notebook.DeletePage(self.notebook.GetPageIndex(tab))

        try:
            self.remote_tabs.remove(tab)
        except ValueError:
            pass

        self.statusbar.SetStatusText('Database closed')

    def on_tab_close(self, event):
        self.remote_tabs.remove(self.notebook.GetCurrentPage())

    def on_connect(self, event):
        server_address = '192.168.0.154:8080'
        user = 'admin'
        password = 'Fanegas08'

        try:
            self.statusbar.SetStatusText('Connecting...')
            self.client.connect(server_address, user, password)
            self.statusbar.update_status()
        except Exception as e:
            self.StatusBar.SetStatusText(str(e))

    def on_disconnect(self, event):
        self.client._authentication = None

        for tab in self.remote_tabs:
            self.notebook.DeletePage(self.notebook.GetPageIndex(tab))

        self.remote_tabs = list()
        self.statusbar.SetStatusText('Logged out')
        self.statusbar.update_status()

    def on_new_remote(self, event):
        dialog = wx.TextEntryDialog(self, 'Name:','New database')

        if dialog.ShowModal() == wx.ID_OK:
            database = dialog.GetValue()
            self._new_tab(self.client.create_remote_database(database), 'Database created', is_local=False)

        dialog.Destroy()

    def on_open_remote(self, event):
        dialog = wx.SingleChoiceDialog(self, 'Database:', 'Open database', sorted(self.client.remote_databases))

        if dialog.ShowModal() == wx.ID_OK:
            database = dialog.GetStringSelection()
            self._new_tab(self.client.load_remote_database(database), 'Database loaded', is_local=False)

        dialog.Destroy()

    def on_info(self, event):
        pass

    def on_sessions(self, event):
        pass

    def on_sync(self, event):
        pass

    def on_shutdown(self, event):
        pass


class TabPanel(wx.Panel):

    def __init__(self, parent, database, is_local=True):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.parent = parent
        self.database = database
        self.is_local = is_local


class CustomStatusBar(wx.StatusBar):

    def __init__(self, parent):
        super().__init__(parent, -1)
        self.parent = parent
        self.SetFieldsCount(2)
        self.SetStatusWidths([-4, -1])

        # Set up the status label
        self.status_label = wx.StaticText(self, -1, '')
        self.update_status()

        # Listen to the resize event.
        self.Bind(wx.EVT_SIZE, self._on_resize)

    def update_status(self):

        if self.parent.client._authentication:
            self.status_label.SetLabel(self.parent.client.session['user'])
        else:
            self.status_label.SetLabel('Not connected')

        self._reposition_status_label()

    def _on_resize(self, event):
        self._reposition_status_label()

    def _reposition_status_label(self):
        # Get the rect of the second field.
        field_rect = self.GetFieldRect(1)
        label_rect = self.status_label.GetRect()

        # Reduce the width of the field rect to the width of the label rect and
        # increase it's x value by the same about. This will result in it being
        # right aligned.
        width_diff = field_rect.width - label_rect.width

        field_rect.width = label_rect.width
        field_rect.x += width_diff

        # On windows, the text is a little too high up, so increase the Y value
        # a little.
        if sys.platform == 'win32':
            field_rect.y += 3

        # Set the resulting rect to the label.
        self.status_label.SetRect(field_rect)

    def write(self, msg):

        if msg != '\n':
            self.SetStatusText(msg.replace('\n', '; '))


def launch_app():
    app = wx.App(False)
    frame = MainWindow(None, 'Loadit')

    # Preload heavy module
    def import_heavy_modules():
        import loadit.queries

    threading.Thread(target=import_heavy_modules).start()
    app.MainLoop()
