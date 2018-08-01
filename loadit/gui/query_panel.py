import wx
import os
import re
import json
from loadit.database import parse_query, write_query


class QueryPanel(wx.Panel):

    def __init__(self, parent, root, database):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        # Table
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Table:'), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        self._table = wx.Choice(self, size=(300, -1))
        self._table.Bind(wx.EVT_CHOICE, self.update_fields)
        field_sizer.Add(self._table, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 5)
        sizer.Add(-1, 8)

        # IDs
        self.IDs_notebook = wx.Notebook(self)

        # By ID tab
        panel = wx.Panel(self.IDs_notebook, id=wx.ID_ANY)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.IDs_label = wx.StaticText(panel, label='IDs:')
        field_sizer.Add(self.IDs_label, 0, wx.ALL + wx.ALIGN_TOP, 5)
        self._IDs = wx.TextCtrl(panel, id=wx.ID_ANY, style= wx.TE_MULTILINE + wx.TE_WORDWRAP, size=(400, 50))
        field_sizer.Add(self._IDs, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(field_sizer)
        self.IDs_notebook.AddPage(panel, 'By ID')

        # By groups tab
        panel = wx.Panel(self.IDs_notebook, id=wx.ID_ANY)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(panel, label='File:'), 0, wx.ALL + wx.ALIGN_LEFT + wx.ALIGN_TOP, 5)
        self._groups_file = wx.TextCtrl(panel, id=wx.ID_ANY, style= wx.TE_READONLY, size=(400, -1))
        self._groups_file.Bind(wx.EVT_TEXT, self.update_buttons)
        field_sizer.Add(self._groups_file, 1, wx.ALL + wx.ALIGN_TOP, 5)
        button = wx.Button(panel, id=wx.ID_ANY, label='Select')
        button.Bind(wx.EVT_BUTTON, self.select_groups_file)
        field_sizer.Add(button, 0, wx.ALL + wx.ALIGN_TOP, 5)
        panel.SetSizer(field_sizer)
        self.IDs_notebook.AddPage(panel, 'By Groups')
        self.IDs_notebook.ChangeSelection(0)
        self.IDs_notebook.Bind(wx.EVT_NOTEBOOK_PAGE_CHANGED, self.update_fields)
        sizer.Add(self.IDs_notebook, 0, wx.ALL + wx.EXPAND, 5)

        # LIDs
        self.LIDs_notebook = wx.Notebook(self)
        self.critical_LIDs = False

        # By LIDs tab
        panel = wx.Panel(self.LIDs_notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.LIDs_label = wx.StaticText(panel, label='LIDs:')
        field_sizer.Add(self.LIDs_label, 0, wx.ALL + wx.ALIGN_TOP, 5)
        self._LIDs = wx.TextCtrl(panel, id=wx.ID_ANY, style= wx.TE_MULTILINE + wx.TE_WORDWRAP, size=(400, 50))
        field_sizer.Add(self._LIDs, 1, wx.ALL + wx.EXPAND, 5)
        panel_sizer.Add(field_sizer, 1, wx.EXPAND)
        self._critical_LIDs = wx.CheckBox(panel, label='Critical Only')
        self._critical_LIDs.Bind(wx.EVT_CHECKBOX, self.update_critical_LIDs)
        panel_sizer.Add(self._critical_LIDs, 0, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        self.LIDs_notebook.AddPage(panel, 'By LIDs')

        # By Combinations tab
        panel = wx.Panel(self.LIDs_notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(panel, label='File:'), 0, wx.ALL + wx.ALIGN_LEFT + wx.ALIGN_TOP, 5)
        self._LIDs_file = wx.TextCtrl(panel, id=wx.ID_ANY, style= wx.TE_READONLY, size=(400, -1))
        self._LIDs_file.Bind(wx.EVT_TEXT, self.update_buttons)
        field_sizer.Add(self._LIDs_file, 1, wx.ALL + wx.ALIGN_TOP, 5)
        button = wx.Button(panel, id=wx.ID_ANY, label='Select')
        button.Bind(wx.EVT_BUTTON, self.select_LIDs_file)
        field_sizer.Add(button, 0, wx.ALL + wx.ALIGN_TOP, 5)
        panel_sizer.Add(field_sizer, 1, wx.EXPAND)
        self._critical_LIDs2 = wx.CheckBox(panel, label='Critical Only')
        self._critical_LIDs2.Bind(wx.EVT_CHECKBOX, self.update_critical_LIDs)
        panel_sizer.Add(self._critical_LIDs2, 0, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        self.LIDs_notebook.AddPage(panel, 'By Combinations')
        self.LIDs_notebook.ChangeSelection(0)
        self.LIDs_notebook.Bind(wx.EVT_NOTEBOOK_PAGE_CHANGED, self.update_buttons)
        sizer.Add(self.LIDs_notebook, 0, wx.ALL + wx.EXPAND, 5)

        # Fields
        fields_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self._fields = dict()
        self.last_used_field = None

        for column in range(2):
            fields_column_sizer = wx.BoxSizer(wx.VERTICAL)

            for rows in range(5):
                field_sizer = wx.BoxSizer(wx.HORIZONTAL)
                field_sizer.Add(wx.StaticText(self, label=f'Field {len(self._fields)}:'), 0, wx.ALL + wx.ALIGN_RIGHT, 5)
                field = wx.Choice(self, size=(125, -1))
                field.Bind(wx.EVT_CHOICE, self.update_fields)
                field_sizer.Add(field, 0, wx.ALL + wx.EXPAND, 1)
                aggregations = wx.Choice(self, size=(125, -1), choices=[])
                field_sizer.Add(aggregations, 0, wx.ALL + wx.EXPAND, 1)
                fields_column_sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 1)
                self._fields[field] = aggregations
            
            fields_sizer.Add(fields_column_sizer, 0, wx.LEFT + wx.RIGHT + wx.EXPAND, 10)

        sizer.Add(fields_sizer, 0, wx.ALL + wx.EXPAND, 5)
        sizer.Add(-1, 8)

        # Geometry file
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Geometry file:', size=(90, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        self._geometry_file = wx.TextCtrl(self, id=wx.ID_ANY, style= wx.TE_READONLY, size=(320, -1))
        field_sizer.Add(self._geometry_file, 0, wx.LEFT + wx.EXPAND, 5)
        button = wx.Button(self, id=wx.ID_ANY, label='Select')
        button.Bind(wx.EVT_BUTTON, self.select_geometry_file)
        field_sizer.Add(button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 5)
        sizer.Add(-1, 8)

        # Output file
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Output file:', size=(90, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        self._output_file = wx.TextCtrl(self, id=wx.ID_ANY, style= wx.TE_READONLY, size=(320, -1))
        self._output_file.Bind(wx.EVT_TEXT, self.update_buttons)
        field_sizer.Add(self._output_file, 0, wx.LEFT + wx.EXPAND, 5)
        button = wx.Button(self, id=wx.ID_ANY, label='Select')
        button.Bind(wx.EVT_BUTTON, self.select_output_file)
        field_sizer.Add(button, 0, wx.LEFT + wx.EXPAND, 5)
        field_sizer.Add(wx.StaticText(self, label='Type:'), 0, wx.LEFT + wx.ALIGN_RIGHT, 25)
        self._output_type = wx.Choice(self, choices=['csv', 'parquet'], size=(80, -1))
        self._output_type.Bind(wx.EVT_CHOICE, self.on_output_type_change)
        field_sizer.Add(self._output_type, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 5)
        sizer.Add(-1, 8)

        # Buttons
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.save_button = wx.Button(self, id=wx.ID_ANY, label='Save')
        self.save_button.Bind(wx.EVT_BUTTON, self.save_query)
        field_sizer.Add(self.save_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.query_button = wx.Button(self, id=wx.ID_ANY, label='Query')
        self.query_button.Bind(wx.EVT_BUTTON, self.do_query)
        self.query_button.SetDefault()
        field_sizer.Add(self.query_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)
        self.update()

    def update(self):

        if self.database.header.tables:
            self._table.SetItems(list(self.database.header.tables))

        self.update_fields(None)

    def update_fields(self, event):

        if self.database.header.tables:
            self.Enabled = True
            table = self.database.header.tables[self._table.GetString(self._table.GetSelection())]
            self.LIDs_label.SetLabel(table['columns'][0][0] + 's:')
            self.IDs_label.SetLabel(table['columns'][1][0] + 's:')
            self.IDs_notebook.SetPageText(0, 'By ' + table['columns'][1][0] + 's')
            fields = [field[0] for field in table['columns'][2:]]
            fields += table['query_functions']
            fields += [f'ABS({field})' for field in fields]

            if event and not event.GetEventObject() is self._table:
                reset_field = False
            else:
                reset_field = True

            if event and event.GetEventObject() is self.IDs_notebook:
                IDs_tab_selection = event.GetSelection()
            else:
                IDs_tab_selection = self.IDs_notebook.GetSelection()

            if event and event.GetEventObject() in self._fields:
                field = event.GetEventObject()
                aggregation = self._fields[field]
                self.set_field(field, aggregation, IDs_tab_selection == 0, [''] + fields, reset_field)
            else:
        
                for field, aggregation in self._fields.items():
                    self.set_field(field, aggregation, IDs_tab_selection == 0, [''] + fields, reset_field)
                
        else:
            self.Enabled = False

        self.update_buttons(event)
            
        if event:
            event.Skip()

    def set_field(self, field, aggregation, by_id, fields, reset_field):

        if reset_field:
            field.SetItems(fields)
            field.SetSelection(0)

        if field.GetString(field.GetSelection()) == '':
            aggregation.SetItems([''])
            aggregation.Enabled = False
        else:
            aggregation.Enabled = True

            if by_id: # By ID

                if not self.critical_LIDs: # All LIDs
                    aggregation.SetItems([''])
                    aggregation.SetSelection(0)
                    aggregation.Enabled = False
                else: # Critical LID
                    aggregation.SetItems(['MAX', 'MIN'])
                    aggregation.SetSelection(0)

            else: # By group
                
                if not self.critical_LIDs: # All LIDs
                    aggregation.SetItems(['AVG', 'ABS(AVG)', 'MAX', 'MIN'])
                    aggregation.SetSelection(0)
                else: # Critical LID
                    aggregation.SetItems(['AVG-MAX', 'ABS(AVG)-MAX', 'AVG-MIN', 'MAX-MAX', 'MIN-MIN'])
                    aggregation.SetSelection(0)

    def update_critical_LIDs(self, event):
        self.critical_LIDs = event.GetEventObject().IsChecked()

        self._critical_LIDs.SetValue(self.critical_LIDs)
        self._critical_LIDs2.SetValue(self.critical_LIDs)
        self.update_fields(event)
    
    def on_output_type_change(self, event):

        if self._output_file.Value:
            self._output_file.Value = os.path.splitext(self._output_file.Value)[0] + '.' + self._output_type.GetString(self._output_type.GetSelection())

        self.update_buttons(None)

    def update_buttons(self, event):

        if event and event.GetEventObject() is self.IDs_notebook:
            IDs_tab_selection = event.GetSelection()
        else:
            IDs_tab_selection = self.IDs_notebook.GetSelection()

        if event and event.GetEventObject() is self.LIDs_notebook:
            LIDs_tab_selection = event.GetSelection()
        else:
            LIDs_tab_selection = self.LIDs_notebook.GetSelection()

        if (self._output_file.Value and
            (IDs_tab_selection == 0 or IDs_tab_selection == 1 and self._groups_file.Value) and
            (LIDs_tab_selection == 0 or LIDs_tab_selection == 1 and self._LIDs_file.Value) and
            any(field.GetString(field.GetSelection()) for field in self._fields)):
            self.save_button.Enabled = True
            self.query_button.Enabled = True
        else:
            self.save_button.Enabled = False
            self.query_button.Enabled = False

        if event:
            event.Skip()

    def select_groups_file(self, event):

        with wx.FileDialog(self.root, 'Select group file', style=wx.FD_DEFAULT_STYLE, wildcard='CSV files (*.csv)|*.csv') as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                self._groups_file.SetValue(dialog.GetPath())

    def select_LIDs_file(self, event):

        with wx.FileDialog(self.root, 'Select LIDs file', style=wx.FD_DEFAULT_STYLE, wildcard='CSV files (*.csv)|*.csv') as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                self._LIDs_file.SetValue(dialog.GetPath())

    def select_geometry_file(self, event):

        with wx.FileDialog(self.root, 'Select geometry file', style=wx.FD_DEFAULT_STYLE, wildcard='CSV files (*.csv)|*.csv') as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                self._geometry_file.SetValue(dialog.GetPath())

    def select_output_file(self, event):

        if self._output_type.GetSelection() == 0:
            wildcard = 'CSV files (*.csv)|*.csv'
        else:
            wildcard = 'PARQUET files (*.parquet)|*.parquet'

        with wx.FileDialog(self.root, 'Select output file', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT, wildcard=wildcard) as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                self._output_file.SetValue(dialog.GetPath())

    def save_query(self, event):

        with wx.FileDialog(self.root, 'Save query file', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT, wildcard='JSON files (*.json)|*.json') as dialog:
    
            if dialog.ShowModal() == wx.ID_OK:
                query = self.get_query()
                
                with open(dialog.GetPath(), 'w') as f:
                    json.dump(query, f, indent=4)
                
                self.root.statusbar.SetStatusText('Query saved')

    def do_query(self, event):
        query = self.get_query()
        write_query(self.database.query(**parse_query(query, True)), query['output_file'])

    def get_query(self):
        query = dict()
        query['output_file'] = self._output_file.GetValue()
        query['table'] = self._table.GetString(self._table.GetSelection())
        query['fields'] = list()

        for field, aggregation in self._fields.items():
            field_value = field.GetString(field.GetSelection())
            
            if field_value:

                if aggregation.GetString(aggregation.GetSelection()):
                    field_value += '-' + aggregation.GetString(aggregation.GetSelection())

                query['fields'].append(field_value)

        if self.LIDs_notebook.GetSelection() == 0:
            query['LIDs'] = [int(x) for x in re.sub(' *', '', self._LIDs.GetValue()).split(',') if x]
        else:
            query['LIDs'] = self._LIDs_file.GetValue()

        if self.IDs_notebook.GetSelection() == 0:
            query['IDs'] = [int(x) for x in re.sub(' *', '', self._IDs.GetValue()).split(',') if x]
            query['groups'] = None
        else:
            query['IDs'] = None
            query['groups'] = self._groups_file.GetValue()

        query['geometry'] = self._geometry_file.GetValue()

        return query
