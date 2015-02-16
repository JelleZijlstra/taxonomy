import cmd


RED = 31
GREEN = 32


class StopException(Exception):
	pass


def _color(code):
    return '%s[%sm' % (chr(27), code)


def _colored_text(text, code):
    return '%s%s%s' % (_color(code), text, _color(0))


def red(text):
    return _colored_text(text, RED)


def green(text):
    return _colored_text(text, GREEN)


def get_line(prompt, validate=None, handlers={}, should_stop=lambda _: False):
	class CmdLoop(cmd.Cmd):
		def default(self, line):
			return

		def postcmd(self, stop, line):
			if line == 'EOF':
				raise StopException()
			if line in handlers:
				return False
			if should_stop(line):
				self.result = None
				return True
			if validate is None or validate(line):
				self.result = line
				return True
			else:
				print('Invalid input')
				return False

	for key, fn in handlers.items():
		setattr(CmdLoop, 'do_%s' % key, lambda self, line: fn(line))

	loop = CmdLoop()
	loop.prompt = '> '
	loop.cmdloop(prompt)
	return loop.result


def yes_no(prompt):
	positive = {'y', 'yes'}
	negative = {'n', 'no'}
	result = get_line(prompt, validate=lambda line: line.lower() in (positive | negative))
	return result.lower() in positive
