#!/usr/bin/env python

import cStringIO
from pprint import pprint

header = """var %(name)s = {
        chart: {
            renderTo: 'picture1',
            type: 'line',
            marginRight: 130,
            marginBottom: 25
        },
        title: {
            text: '%(title)s',
            x: -20
        },
        tooltip: {
            formatter: function() { return '<b>'+ this.series.name +'</b><br/>'+ ' x: ' + this.x + ' y: '+ this.y;}
        },
        legend: {
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'top',
            x: -10,
            y: 100,
            borderWidth: 0
        },
"""
xaxis = """        xAxis: {
            title: {
                text: '%(xlabel)s'
            },
            categories: %(cat)s
        },
"""
yaxis = """        yAxis: {
            title: {
                text: '%(ylabel)s'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
"""
series = """series: [%(ans)s]
    }"""

each = """, {
            name: '%(db)s',
            data: %(data)s
        }"""

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


class HC:
    DB = [  ('rds', 'Redis'), 
            ('tnt', 'Tarantool'), 
            ('mongodb', 'MongoDB'), 
            ('memcached', 'Memcached')
    ]

    WLS = [ 
        ('workloada', 'Workload A', 'A'),
        ('workloadb', 'Workload B', 'B'),
        ('workloadc', 'Workload C', 'C'),
        ('workloadd', 'Workload D', 'D'),
        ('workloade', 'Workload E', 'E'),
        ('workloadf', 'Workload F', 'F')
    ]

    def __init__(self, name):
        self.name = name
        self.data = []

    def set_xtics(self, itrbl):
        self._xtics = tuple(itrbl)
        return self

    def set_title(self, title, xlabel, ylabel):
        for i in self.WLS:
            title = title.replace(i[0], i[1])
        self._file = ((header + xaxis + yaxis) % {
                "title"  : str(title),
                "xlabel" : str(xlabel),
                "ylabel" : str(ylabel),
                "cat"  : tuple(self._xtics),
                "name" : str(self.name)
                }) + series
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
        return string # TODO
