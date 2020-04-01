# coding: utf8
from datetime import date, time
import sys

from asciimatics.exceptions import StopApplication, ResizeScreenError
from asciimatics.scene import Scene
from asciimatics.screen import Screen
from asciimatics.widgets import Button, CheckBox, DatePicker, Frame, Layout, ListBox, PopUpDialog, Text, TimePicker, \
    Divider, Label

from classes.constants import CHANNELS_TO_WAVELENGTHS, INTERFACE_PALETTE
from classes.interface.main_controller import MainController


# shamelessly work around the lack of form state with a static store
class FormStateData(object):
    data = {
        'before_date_range_start': date(2002, 6, 1),
        'before_date_range_end': date(2016, 6, 1),
        'after_date_range_start': date(2002, 6, 1),
        'after_date_range_end': date(2016, 6, 1),
        'time_range_start': time(16, 0),
        'time_range_end': time(20, 0),
        'channel': 0,
    }


class CheckBoxesGroup(object):
    """Provides logic to ensure at least one checkbox in a group is always checked"""

    def __init__(self, checkboxes, default_selection):
        self.checkboxes = checkboxes
        self.default = default_selection

        self._assign_onchange_handlers()

    def _assign_onchange_handlers(self):
        def handler():
            if not any(map(lambda x: x.value, self.checkboxes)):
                self.default.value = True

        for checkbox in self.checkboxes:
            checkbox.set_onchange(handler)


class SetterMixin(object):
    def __init__(self):
        self._validator = None
        self._on_change = None

    def set_validator(self, validator):
        self._validator = validator

    def set_onchange(self, handler):
        self._on_change = handler


class CustomText(Text, SetterMixin):
    pass


class CustomCheckBox(CheckBox, SetterMixin):
    pass


class CustomPopupDialog(PopUpDialog):
    # Override standard palette for pop-ups
    _normal = (Screen.COLOUR_BLACK, Screen.A_NORMAL, Screen.COLOUR_WHITE)
    _bold = (Screen.COLOUR_BLACK, Screen.A_BOLD, Screen.COLOUR_WHITE)
    _focus = (Screen.COLOUR_BLACK, Screen.A_BOLD, Screen.COLOUR_GREEN)
    palette = {
        "background": _normal,
        "shadow": (Screen.COLOUR_BLACK, Screen.A_NORMAL, Screen.COLOUR_BLACK),
        "label": _bold,
        "borders": _normal,
        "scroll": _normal,
        "title": _bold,
        "edit_text": _normal,
        "focus_edit_text": _bold,
        "field": _normal,
        "focus_field": _bold,
        "button": _normal,
        "focus_button": _focus,
        "control": _normal,
        "focus_control": _bold,
        "disabled": _bold,
    }


class AsciiView(Frame):
    def __init__(self, screen):
        super().__init__(
            screen, screen.height, 80, data=FormStateData.data, title="Aqua AIRS HDF Processing", reduce_cpu=False
        )

        self.palette = INTERFACE_PALETTE
        self.controller = MainController(self.status_callback)

        # supporting variables
        wavelengths = list(reversed([(str(w), c) for c, w in CHANNELS_TO_WAVELENGTHS.items()]))
        year_range = range(2002, 2016 + 1)

        # declare controls
        self.username = Text('EarthData Login username: ', 'username')
        self.password = Text('EarthData Login password: ', 'password', hide_char='*')
        self.output_directory = Text('Output directory: ', 'output_directory')
        self.data_directory = Text('HDF data directory: ', 'data_directory')
        self.before_date_range_start = DatePicker('"Before" date range start:', 'before_date_range_start',
                                                  year_range=year_range)
        self.before_date_range_end = DatePicker('"Before" date range end:', 'before_date_range_end',
                                                year_range=year_range)
        self.after_date_range_start = DatePicker('"After" date range start:', 'after_date_range_start',
                                                 year_range=year_range)
        self.after_date_range_end = DatePicker('"After" date range end:', 'after_date_range_end',
                                               year_range=year_range)
        self.time_range_start = TimePicker('Start time: ', name='time_range_start')
        self.time_range_end = TimePicker('End time : ', name='time_range_end')
        self.min_latitude = CustomText('Minimum latitude: ', name='min_latitude')
        self.max_latitude = CustomText('Maximum latitude: ', name='max_latitude')
        self.min_longitude = CustomText('Minimum longitude: ', name='min_longitude')
        self.max_longitude = CustomText('Maximum longitude: ', name='max_longitude')
        self.use_radiance_filters = CheckBox('Use radiance filter', 'Radiance filter', name='use_radiance_filters')
        self.radiance = CustomText('Radiance: ', name='radiance')
        self.radiance_range = CustomText('Radiance range (mW): ±', name='radiance_range')
        self.channel = ListBox(1, wavelengths, label='Wavelength:', name='channel')  # value is channel, not wavelength
        self.max_landfrac = CustomText('Maximum landfrac (0-1): ', name='max_landfrac')
        self.dust_flag_no_dust = CustomCheckBox('No dust', 'Dust presence', name='dust_flag_no_dust')
        self.dust_flag_single_fov = CustomCheckBox('Dust in a single FOV', name='dust_flag_single_fov')
        self.dust_flag_detected = CustomCheckBox('Dust detected', name='dust_flag_detected')
        self.data_quality_best = CustomCheckBox('Best', 'Data quality level', name='data_quality_best')
        self.data_quality_enough = CustomCheckBox('Good enough', name='data_quality_enough')
        self.data_quality_worst = CustomCheckBox('Worst', name='data_quality_worst')
        self.center_scans_only = CheckBox('Use center IR scans only', 'Other', name='center_scans_only')
        self.noise_amp = CheckBox('Consider noise amplification', name='noise_amp')
        self.progress_label = Label('')

        self.button_process = Button('Process', self._process)
        self.button_quit = Button('Quit', self._quit)

        # declare layouts
        top_layout = Layout([1])
        time_layout = Layout([1])
        radiance_layout = Layout([1])
        options_layout = Layout([1])
        buttons_layout = Layout([1, 1, 1, 1])
        status_layout = Layout([1])

        # attach layouts to self
        self.add_layout(top_layout)
        self.add_layout(time_layout)
        self.add_layout(radiance_layout)
        self.add_layout(options_layout)
        self.add_layout(buttons_layout)
        self.add_layout(status_layout)

        # attach controls to layouts
        top_layout.add_widget(self.username)
        top_layout.add_widget(self.password)
        top_layout.add_widget(Divider(False))
        top_layout.add_widget(self.data_directory)
        top_layout.add_widget(self.output_directory)
        top_layout.add_widget(Divider(False))
        top_layout.add_widget(self.before_date_range_start)
        top_layout.add_widget(self.before_date_range_end)
        top_layout.add_widget(self.after_date_range_start)
        top_layout.add_widget(self.after_date_range_end)

        time_layout.add_widget(self.time_range_start)
        time_layout.add_widget(self.time_range_end)
        time_layout.add_widget(Divider(False))
        time_layout.add_widget(self.min_latitude)
        time_layout.add_widget(self.max_latitude)
        time_layout.add_widget(self.min_longitude)
        time_layout.add_widget(self.max_longitude)
        time_layout.add_widget(Divider(False))

        radiance_layout.add_widget(self.use_radiance_filters)
        radiance_layout.add_widget(self.radiance)
        radiance_layout.add_widget(self.radiance_range)
        radiance_layout.add_widget(self.channel)
        radiance_layout.add_widget(Divider(False))

        options_layout.add_widget(self.max_landfrac)
        options_layout.add_widget(self.dust_flag_no_dust)
        options_layout.add_widget(self.dust_flag_single_fov)
        options_layout.add_widget(self.dust_flag_detected)
        options_layout.add_widget(Divider(False))
        options_layout.add_widget(self.data_quality_best)
        options_layout.add_widget(self.data_quality_enough)
        options_layout.add_widget(self.data_quality_worst)
        options_layout.add_widget(Divider(False))
        options_layout.add_widget(self.center_scans_only)
        options_layout.add_widget(self.noise_amp)

        options_layout.add_widget(Divider(False))
        options_layout.add_widget(Divider(False))
        buttons_layout.add_widget(self.button_process, 2)
        buttons_layout.add_widget(self.button_quit, 3)

        status_layout.add_widget(Divider(False))
        status_layout.add_widget(Divider(False))
        status_layout.add_widget(self.progress_label)

        # add validating regexes
        self.min_latitude.set_validator('^-?(?:\d+(?:\.\d+)?|\.\d+)$')
        self.max_latitude.set_validator('^-?(?:\d+(?:\.\d+)?|\.\d+)$')
        self.min_longitude.set_validator('^-?(?:\d+(?:\.\d+)?|\.\d+)$')
        self.max_longitude.set_validator('^-?(?:\d+(?:\.\d+)?|\.\d+)$')
        self.radiance.set_validator('^(?:\d+(?:\.\d+)?|\.\d+)$')
        self.radiance_range.set_validator('^(?:\d+(?:\.\d+)?|\.\d+)$')
        self.max_landfrac.set_validator('^(?:[01]+(?:\.\d+)?)|\.\d+$')

        # checkboxes groups with default values
        self.dust_flag_group = CheckBoxesGroup(
            [self.dust_flag_no_dust, self.dust_flag_single_fov, self.dust_flag_detected],
            self.dust_flag_no_dust
        )
        self.data_quality_group = CheckBoxesGroup(
            [self.data_quality_best, self.data_quality_enough, self.data_quality_worst],
            self.data_quality_enough
        )

        self.fix()

    def status_callback(self, message, done, _):
        self.progress_label.text = message

        self.screen.draw_next_frame()

        self.button_quit.disabled = not done
        self.button_process.disabled = not done

    def _quit(self):
        self._scene.add_effect(
            CustomPopupDialog(self._screen, "Quit?", ["No", "Yes"], on_close=self._quit_on_yes))

    def _process(self):
        self.save()
        self.controller.process(self.data)

    @staticmethod
    def _quit_on_yes(selected):
        if selected == 1:  # second button
            raise StopApplication("Normal termination")


if __name__ == '__main__':
    def main(screen, scene):
        ascii_view = AsciiView(screen)

        scenes = [
            Scene([ascii_view], -1, name="Main"),
        ]
        try:
            screen.play(scenes, stop_on_resize=True, start_scene=scene)
        except ResizeScreenError:
            ascii_view.save()
            FormStateData.data = ascii_view.data
            raise


    while True:
        try:
            Screen.wrapper(main, catch_interrupt=True, arguments=[None])
            sys.exit(0)
        except ResizeScreenError as e:
            last_scene = e.scene
