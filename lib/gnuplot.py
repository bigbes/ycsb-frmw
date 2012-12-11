#!/usr/bin/env python

import cStringIO
from pprint import pprint

header = """set output '%(output)s'
%(xtics)s
set ytics border in scale 1,0.5 mirror norotate offset character 0,0,0
set title "%(title)s"
set xlabel "%(xlabel)s"
set ylabel "%(ylabel)s"
"""

terminal_png = """set terminal png nocrop enhanced size 1024.600 fon Arial 9
"""

terminal_svg = """set terminal svg enhanced size 1280,1024 fname 'Arial' fsize 9
"""

_xrange = """set xrange [0 : %(end)f]
"""

_yrange = """set yrange [0 : %(end)f]
"""

_plot = """\t'%(file)s' using 1:2 title "%(title)s" with lines, """
_plot_additional = """'' using 1:2:(sprintf("%.2f", $2)) with labels offset 0,0.5 font "Arial,7" notitle"""

__plot = """\t'%(file)s' using 1:2 with lines t "%(title)s", \\
\t'%(file)s' using 1:2:2 with labels notitle"""

_obj = { 'svg' : terminal_svg,
        'png' : terminal_png
}

##########################Example
#   >>>from gnuplot import Plot
#   >>> Plot('lol', 'out').set_title('fuck this title', 'fuck xaxis', 'fuck yaxis').add_data('file1.data', 
#   ...'tarantool').add_data('file2.data', 'redis').gen_file()                                    
#   set terminal svg enhanced size 1024,600 fname 'Arial' fsize 9
#   set output out.svg
#   set ytics border in scale 1,0.5 mirror norotate offset character 0,0,0
#   set title "fuck this title"
#   set xlabel "fuck xaxis"
#   set ylabel "fuck yaxis"
#   
#   plot    'file1.data' using 1:2 with lines t "tarantool", \
#           'file2.data' using 1:2 with lines t "redis"


class Plot:
    DB = [  ('rds', 'Redis'), 
            ('tnt', 'Tarantool'), 
            ('mongodb', 'MongoDB'), 
            ('memcached', 'Memcached')
    ]

    WLS = [ 
        ('workloada', 'Workload A'),
        ('workloadb', 'Workload B'),
        ('workloadc', 'Workload C'),
        ('workloadd', 'Workload D'),
        ('workloade', 'Workload E'),
        ('workloadf', 'Workload F')
    ]

    def __init__(self, _file, output, _format = 'svg'):
        if not _format in _obj:
            _format = 'svg'
        self.o_file = _file
        self.data = []
        self._format = _format
        self.output = output


    def set_xmax(self, xmax):
        self.xmax = float(xmax)
        self._file += _xrange % {"end" : float(xmax)}
        return self
    
    def set_ymax(self, ymax):
        self.ymax = float(ymax)
        self._file += _yrange % {"end" : float(ymax)}
        return self
    
    def set_xtics(self, itrbl):
        self._xtics = tuple(itrbl)
        return self

    def set_title(self, title, xlabel, ylabel, with_xtics = None):
        for i in self.WLS:
            title = title.replace(i[0], i[1])
        self._file = (_obj[self._format] + header) % {
                "output" : str(self.output)+"."+str(self._format), 
                "title"  : str(title), 
                "xlabel" : str(xlabel),
                "ylabel" : str(ylabel),
                "xtics"  : ("set xtics " + str(tuple(with_xtics)).replace(' ', '')) if with_xtics else ''
                }   
        return self

    def add_data(self, _file, title):
        title = title.replace('_', ' ')
        for i in self.DB:
            title = title.replace(i[0], i[1])
        self.data.append((_file, title))
        return self

    def gen_file(self):
        f = cStringIO.StringIO()
        temp = open(self.o_file, 'w')
        f.write(self._file + '\nplot')
        for i in self.data[0:-1]:
            f.write(_plot % {"file" : str(i[0]), "title" : str(i[1])} + _plot_additional + ',\\\n')
        f.write(_plot % {"file" : str(self.data[-1][0]), "title" : str(self.data[-1][1])} + _plot_additional + '\n')
        temp.write(f.getvalue())
        temp.close();f.close();
