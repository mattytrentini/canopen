_MISSING = object()


class NoSectionError(Exception):
    pass


class NoOptionError(Exception):
    pass


class RawConfigParser:
    """Minimal INI parser covering the surface used by canopen/objectdictionary/eds.py."""

    def __init__(self, inline_comment_prefixes=None):
        self._sections = {}
        self._comments = inline_comment_prefixes or ()
        self.optionxform = str.lower

    def read_file(self, fp):
        section = None
        for line in fp:
            line = line.strip()
            if not line or line[0] in (";", "#"):
                continue
            if line[0] == "[" and line[-1] == "]":
                section = line[1:-1]
                self._sections.setdefault(section, {})
            elif "=" in line and section is not None:
                key, _, val = line.partition("=")
                for pfx in self._comments:
                    idx = val.find(pfx)
                    if idx != -1:
                        val = val[:idx]
                self._sections[section][self.optionxform(key.strip())] = val.strip()

    def sections(self):
        return list(self._sections)

    def has_section(self, section):
        return section in self._sections

    def options(self, section):
        if section not in self._sections:
            raise NoSectionError(section)
        return list(self._sections[section])

    def has_option(self, section, option):
        return section in self._sections and self.optionxform(option) in self._sections[section]

    def get(self, section, option, fallback=_MISSING):
        if section not in self._sections:
            if fallback is not _MISSING:
                return fallback
            raise NoSectionError(section)
        key = self.optionxform(option)
        if key not in self._sections[section]:
            if fallback is not _MISSING:
                return fallback
            raise NoOptionError(option, section)
        return self._sections[section][key]

    def getint(self, section, option, fallback=_MISSING):
        val = self.get(section, option, fallback=fallback)
        if val is fallback:
            return val
        return int(val, 0)

    def add_section(self, section):
        self._sections[section] = {}

    def set(self, section, option, value):
        if section not in self._sections:
            raise NoSectionError(section)
        self._sections[section][self.optionxform(option)] = str(value)

    def write(self, dest, space_around_delimiters=True):
        sep = " = " if space_around_delimiters else "="
        for section, opts in self._sections.items():
            dest.write("[{}]\n".format(section))
            for key, val in opts.items():
                dest.write("{}{}{}\n".format(key, sep, val))
            dest.write("\n")
