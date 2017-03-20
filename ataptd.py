"""

Disk ATA class

"""

from atapt import atapt
# import ctypes
import sys


class DiskATA(object):
    def __init__(self, path):
        self.path = path
        self.disk = atapt.atapt(path)

    def get_info(self):
        return {
            "device": self.disk.dev,
            "model": self.disk.model,
            "firmware": self.disk.firmware,
            "serial": self.disk.serial,
            "sectors": self.disk.sectors,
            "size": self.disk.size
        }

    def get_smart(self):
        self.disk.timeout = 1000
        self.disk.readSmart()
        data = []
        for id in sorted(self.disk.smart):
            data.append({"id": id,
                         "name": self.disk.getSmartStr(id),
                         "pre_fail": "Pre-fail" if self.disk.smart[id][0] else "Old_age",
                         "online": "Always" if self.disk.smart[id][1] else "Offline",
                         "current": "  %03d" % self.disk.smart[id][2],
                         "worst": "  %03d" % self.disk.smart[id][3],
                         "raw": "  %03d" % self.disk.smart[id][5],
                         "treshold": self.disk.getSmartRawStr(id)})

        return data


if __name__ == "__main__":
    d = DiskATA(sys.argv[1])

    print("{0}".format(d.get_info()))
    print("{0}".format(d.get_smart()))
