import wx
import os
import json
from loadit.log import custom_logging
import logging


log = logging.getLogger()


class MultipleQueryPanel(wx.Panel):

    def __init__(self, parent, root, database, log):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        self.log = log
        self.queries = list()

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        self._queries = wx.ListCtrl(self, wx.ID_ANY, style=wx.LC_REPORT)
        self._queries.InsertColumn(0, 'Query File', width=300)
        self._queries.InsertColumn(1, 'Output File', width=300)
        self._queries.InsertColumn(2, 'Status', width=100)
        self._queries.Bind(wx.EVT_LIST_ITEM_RIGHT_CLICK, self.show_menu)
        sizer.Add(self._queries, 1, wx.ALL + wx.EXPAND, 5)

        # Buttons
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.clear_button = wx.Button(self, id=wx.ID_ANY, label='Clear')
        self.clear_button.Bind(wx.EVT_BUTTON, self.clear_all)
        field_sizer.Add(self.clear_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.add_button = wx.Button(self, id=wx.ID_ANY, label='Add')
        self.add_button.Bind(wx.EVT_BUTTON, self.add_query)
        field_sizer.Add(self.add_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.query_button = wx.Button(self, id=wx.ID_ANY, label='Query')
        self.query_button.Bind(wx.EVT_BUTTON, self.do_queries)
        self.query_button.SetDefault()
        field_sizer.Add(self.query_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)
        self.update()

    def update(self):

        if self.database.header.tables:
            self.Enabled = True
            
            if self.queries:
                self.clear_button.Enabled = True
                self.query_button.Enabled = True
            else:
                self.clear_button.Enabled = False
                self.query_button.Enabled = False
                
        else:
            self.Enabled = False

    def add_query(self, event):

        with wx.FileDialog(self.root, 'Add query', style=wx.FD_OPEN + wx.FD_FILE_MUST_EXIST + wx.FD_MULTIPLE, wildcard='JSON files (*.json)|*.json') as dialog:
        
            if dialog.ShowModal() == wx.ID_OK:

                for query_file in dialog.GetPaths():

                    if query_file not in self.queries:
                        self.queries.append(query_file)

                        with open(query_file) as f:
                            query = json.load(f)
                            self._queries.InsertItem(len(self.queries) - 1, query_file)

                            try:
                                self._queries.SetItem(len(self.queries) - 1, 1, query['output_file'])
                            except KeyError:
                                self._queries.SetItem(len(self.queries) - 1, 1, 'N/A')

                            self._queries.SetItem(len(self.queries) - 1, 2, 'Pending')
                    else:
                        log.info('Query file already selected!')
        
                self.update()

    def remove_query(self, event):
        item = self._queries.GetFirstSelected()

        while item != -1:
            self.queries.remove(self._queries.GetItemText(item, 0))
            self._queries.DeleteItem(item)
            item = self._queries.GetFirstSelected()

        self.update()

    def clear_all(self, event):

        with wx.MessageDialog(self.root, 'Are you sure?', 'Clear queries', style=wx.YES_NO + wx.NO_DEFAULT) as dialog:

            if dialog.ShowModal() == wx.ID_YES:
                self.queries = list()
                self._queries.DeleteAllItems()
                self.update()
                log.info('All queries cleared')

    @custom_logging
    def do_queries(self, event):
        
        for i, query_file in enumerate(self.queries):
            log.info(f"Performing query '{os.path.basename(query_file)}' ({i + 1} of {len(self.queries)})...")
            self._queries.SetItem(i, 2, 'In progress ...')
            self.database.query_from_file(query_file)
            self._queries.SetItem(i, 2, 'Done')

        log.info('Done!')

    def show_menu(self, event):

        if self._queries.GetFirstSelected() != -1:
            popupmenu = wx.Menu()
            menu_item = popupmenu.Append(wx.ID_ANY, 'Remove')
            self._queries.Bind(wx.EVT_MENU, self.remove_query, menu_item)
            self._queries.PopupMenu(popupmenu, event.GetPoint())