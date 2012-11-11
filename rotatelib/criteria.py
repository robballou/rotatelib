import re
import collections
import datetime
import inspect

class BaseCriteria(object):
    criteria_name = None

    def __init__(self, debug = False):
        self.debugMode = False

    def debug(self, message):
        if not self.debugMode:
            return
        print message

    def debugArguments(self, filename, parsed_name):
        if not self.debugMode:
            return

        print "======\nArguments (%s):" % (self.__class__.__name__)
        print "Filename:"
        print filename
        print "Parsed name:"
        print parsed_name
        print "Argument:"
        print self.argument

    def make_list(self, item):
        if isinstance(item, basestring) or not isinstance(item, collections.Iterable):
            item = [item]
        return item

    def set_argument(self, argument):
        self.argument = argument

    def test(self, filename, parsed_name):
        return False

class ListArgumentCriteria(BaseCriteria):
    def set_argument(self, argument):
        self.argument = self.make_list(argument)


class Before(BaseCriteria):
    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        # the name does not contain a parseable date
        if not parsed_name['date']:
            return False

        # check if this is a timedelta object
        try:
            if self.argument.days:
                self.argument = datetime.datetime.today() - self.argument
        except AttributeError:
            pass
        if self.argument and parsed_name['date'] >= self.argument:
            self.debug("FAILED before criteria")
            return False
        return True


class Day(ListArgumentCriteria):
    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        # ignore any day besides the requested on
        if parsed_name['date'] and parsed_name['date'].day not in self.argument:
            self.debug("FAILED day criteria")
            return False
        self.debug("PASSES day criteria")
        return True


class ExceptDay(Day):
    criteria_name = "except_day"
    def test(self, filename, parsed_name):
        meets_day = super(Day, self).test(filename, parsed_name)
        if meets_day:
            self.debug("FAILED except_day criteria")
        else:
            self.debug("PASSES except_day criteria")
        return not meets_day


class HasDate(BaseCriteria):
    criteria_name = "has_date"
    def __init__(self, **kwargs):
        super(HasDate, self).__init__(kwargs)
        self.argument = True

    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        should_have_date = self.argument
        if 'date' not in parsed_name:
            parsed_name['date'] = None

        # determine if the item DOES have a date
        if should_have_date and not parsed_name['date']:
            self.debug("FAILED has_date criteria")
            return False
        elif not should_have_date and parsed_name['date']:
            self.debug("FAILED has_date criteria")
            return False
        self.debug("PASSES has_date criteria")
        return True

class Pattern(BaseCriteria):
    def test(self, filename, parsed_name):
        return re.match(self.argument, filename)

class Startswith(ListArgumentCriteria):
    def test(self, filename, parsed_name):
        passes = False
        for s in self.argument:
            if filename.startswith(s):
                passes = True
                break
        return passes

class ExceptStartswith(Startswith):
    criteria_name = "except_startswith"
    def test(self, filename, parsed_name):
        return not super(ExceptStartswith, self).test(filename, parsed_name)