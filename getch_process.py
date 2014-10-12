from multiprocessing import Manager, Value, Process
from ctypes import c_char_p
import getch

manager = Manager()
global keych
keych = manager.Value(c_char_p, "")

class GetchProcess(Process):
    def __init__(self, keych):
        super(self.__class__, self).__init__()
        self.keych = keych

    def run(self):
        keych.value = getch.getch() # Does not handle all keys properly
