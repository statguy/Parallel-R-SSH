from multiprocessing import Process
import getch

class GetchProcess(Process):
    def __init__(self, keych):
        super(self.__class__, self).__init__()
        self.keych = keych

    def run(self):
        keych.value = getch.getch() # Does not handle all keys properly
