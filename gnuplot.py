#!/usr/bin/env python

from pprint import pprint

header = """set output %(output)s
set ytics border in scale 1,0.5 mirror norotate offset character 0,0,0
set title "%(title)s"
set xlabel "%(xlabel)s"
set ylabel "%(ylabel)s"
"""

terminal_png = """set terminal png nocrop enhanced size 1024.600 fon Arial,9
"""

terminal_svg = """set terminal svg enhanced size 1024,600 fname 'Arial' fsize 9
"""

_xrange = """set xrange [0 : %(end)f]
"""

_yrange = """set yrange [0 : %(end)f]
"""

_plot = """\t'%(file)s' using 1:2 with lines t "%(title)s", \\
\t'%(file)s' using 1:2:2 with labels notitle"""

_obj = { 'svg' : terminal_svg,
		'png' : terminal_png
}

##########################Example
#	>>>from gnuplot import Plot
#	>>> Plot('lol', 'out').set_title('fuck this title', 'fuck xaxis', 'fuck yaxis').add_data('file1.data', 
#	...'tarantool').add_data('file2.data', 'redis').gen_file()                                    
#	set terminal svg enhanced size 1024,600 fname 'Arial' fsize 9
#	set output out.svg
#	set ytics border in scale 1,0.5 mirror norotate offset character 0,0,0
#	set title "fuck this title"
#	set xlabel "fuck xaxis"
#	set ylabel "fuck yaxis"
#	
#	plot    'file1.data' using 1:2 with lines t "tarantool", \
#	        'file1.data' using 1:2:2 with labels notitle, \
#	        'file2.data' using 1:2 with lines t "redis", \
#	        'file2.data' using 1:2:2 with labels notitle

class Plot:
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

	def set_title(self, title, xlabel, ylabel):
		self._file = (_obj[self._format] + header) % {
				"output" : str(self.output)+"."+str(self._format), 
				"title"  : str(title), 
				"xlabel" : str(xlabel),
				"ylabel" : str(ylabel)
				}	
		return self

	def add_data(self, _file, title):
		self.data.append((_file, title))
		return self

	def gen_file(self):
		f = '\nplot '
		for i in self.data[0:-1]:
			f += _plot % {"file" : str(i[0]), "title" : str(i[1])} + ', \\\n'
		f += _plot % {"file" : str(self.data[-1][0]), "title" : str(self.data[-1][1])} + '\n'
		open(self.o_file, 'w').write(self._file + f)
