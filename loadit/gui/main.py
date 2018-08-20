import os
import sys
import threading
import wx
import wx.aui
from loadit.client import Client
from loadit.gui.statusbar import CustomStatusBar
from loadit.gui.connect_dialog import ConnectDialog
from loadit.gui.database_tab import DatabaseTab
import loadit.log
import logging


log = logging.getLogger()


class MainWindow(wx.Frame):

    def __init__(self, parent, title):
        size = (1080, 720)
        super().__init__(parent, title=title, size=size)
        self.SetMinSize(size)
        self.client = Client()

        # Statusbar
        self.statusbar = CustomStatusBar(self)
        self.SetStatusBar(self.statusbar)
        loadit.log.add_handler(logging.StreamHandler(self.statusbar), format='%(message)s')
        loadit.log.disable_console()

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
        self.filemenu_exit = self.filemenu.Append(wx.ID_EXIT, 'Exit', 'Exit the program')
        self.Bind(wx.EVT_MENU, self.on_new, self.filemenu_new)
        self.Bind(wx.EVT_MENU, self.on_open, self.filemenu_open)

        # Remote menu
        self.remotemenu = wx.Menu()
        self.menubar.Append(self.remotemenu, '&Remote')
        self.remotemenu_connect = self.remotemenu.Append(wx.ID_ANY, 'Connect', 'Connect to the remote server')
        self.remotemenu_logout = self.remotemenu.Append(wx.ID_ANY, 'Log Out', 'Log out session')
        self.remotemenu.AppendSeparator()
        self.remotemenu_new = self.remotemenu.Append(wx.ID_ANY, 'New', 'Create a new remote database')
        self.remotemenu_open= self.remotemenu.Append(wx.ID_ANY, 'Open', 'Open an existing remote database')
        self.remotemenu.AppendSeparator()
        self.Bind(wx.EVT_MENU, self.on_connect, self.remotemenu_connect)
        self.Bind(wx.EVT_MENU, self.on_logout, self.remotemenu_logout)
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
        # Remote menu
        self.remotemenu_connect.Enable(True)
        self.remotemenu_logout.Enable(False)
        self.remotemenu_new.Enable(False)
        self.remotemenu_open.Enable(False)
        self.remotemenu_management.Enable(False)

        if self.client._authentication:
            session = self.client.session
            self.remotemenu_connect.Enable(False)
            self.remotemenu_logout.Enable(True)

            if session['create_allowed']:
                self.remotemenu_new.Enable(True)

            self.remotemenu_open.Enable(True)

            if session['is_admin']:
                self.remotemenu_management.Enable(True)

    def _new_tab(self, func, database, msg=None, is_local=True):

        try:
            tab = DatabaseTab(self.notebook, self, func(database), is_local)
            self.notebook.AddPage(tab, os.path.basename(database))
            self.notebook.SetSelection(self.notebook.GetPageIndex(tab))
            self.notebook.SetPageToolTip(self.notebook.GetPageIndex(tab), database)

            if not is_local:
                self.remote_tabs.append(tab)

            if msg:
                log.info(msg)

        except Exception as e:
            log.error(str(e))

    def on_new(self, event):

        with wx.DirDialog(self, 'New database', '', style=wx.DD_DEFAULT_STYLE) as dialog:

            if dialog.ShowModal() == wx.ID_OK:

                with wx.TextEntryDialog(self, 'Name:','New database') as name_dialog:

                    if name_dialog.ShowModal() == wx.ID_OK:
                        database = os.path.join(dialog.GetPath(), name_dialog.GetValue())
                        self._new_tab(self.client.create_database, database, 'Database created', is_local=True)

    def on_open(self, event):

        with wx.DirDialog(self, 'Open database', '', style=wx.DD_DEFAULT_STYLE) as dialog:

            if dialog.ShowModal() == wx.ID_OK:
                database = dialog.GetPath()
                self._new_tab(self.client.load_database, database, is_local=True)

    def on_tab_close(self, event):

        try:
            self.remote_tabs.remove(self.notebook.GetCurrentPage())
        except ValueError:
            pass

        log.info('Database closed')

    def on_connect(self, event):

        with ConnectDialog(self) as dialog:

            if dialog.ShowModal() == wx.ID_OK:

                try:
                    log.info('Connecting...')
                    self.client.connect(dialog.address, dialog.user, dialog.password)
                    self.statusbar.update_status()
                except Exception as e:
                    log.error(str(e))

    def on_logout(self, event):
        self.client.logout()

        for tab in self.remote_tabs:
            self.notebook.DeletePage(self.notebook.GetPageIndex(tab))

        self.remote_tabs = list()
        log.info('Logged out')
        self.statusbar.update_status()

    def on_new_remote(self, event):

        with wx.TextEntryDialog(self, 'Name:','New database') as dialog:

            if dialog.ShowModal() == wx.ID_OK:
                database = dialog.GetValue()
                self._new_tab(self.client.create_remote_database, database, 'Database created', is_local=False)

    def on_open_remote(self, event):
        
        with wx.SingleChoiceDialog(self, 'Database:', 'Open database', sorted(self.client.remote_databases)) as dialog:

            if dialog.ShowModal() == wx.ID_OK:
                database = dialog.GetStringSelection()
                self._new_tab(self.client.load_remote_database, database, 'Database loaded', is_local=False)

    def on_info(self, event):
        pass

    def on_sessions(self, event):
        pass

    def on_sync(self, event):
        pass

    def on_shutdown(self, event):
        pass


def launch_app():
    app = wx.App(False)
    MainWindow(None, 'Loadit')

    # Preload heavy module
    def import_heavy_modules():
        import loadit.queries

    threading.Thread(target=import_heavy_modules).start()
    app.MainLoop()
