import sys
import wx


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