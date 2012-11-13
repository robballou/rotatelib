import re
import collections
import datetime
import inspect

# ---------------------------------------------------------------------
# PARENT/SUPERCLASS CRITERIA
# ---------------------------------------------------------------------


class BaseCriteria(object):
    """
    The base criteria used for all criteria
    """
    criteria_name = None

    def __init__(self, debug=False):
        self.debugMode = False

    def debug(self, message):
        if not self.debugMode:
            return
        print message

    def debugArguments(self, filename, parsed_name):
        if not self.debugMode:
            return

    def make_list(self, item):
        """
        Convert the item to a list, if it isn't one. Useful in cases where
        the criteria needs to act on multiple items.
        """
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

# ---------------------------------------------------------------------
# DATE CRITERIA
# ---------------------------------------------------------------------


class DateCriteria(BaseCriteria):
    def set_argument(self, argument):
        self.argument = argument
        try:
            if self.argument.days:
                self.argument = datetime.datetime.today() - self.argument
        except AttributeError:
            pass

    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        # the name does not contain a parseable date
        if not parsed_name['date']:
            return False
        return True


class After(DateCriteria):
    def test(self, filename, parsed_name):
        if not super(After, self).test(filename, parsed_name):
            return False

        if self.argument and parsed_name['date'] <= self.argument:
            self.debug("FAILED before criteria")
            return False
        return True


class Before(DateCriteria):
    def test(self, filename, parsed_name):
        if not super(Before, self).test(filename, parsed_name):
            return False

        if self.argument and parsed_name['date'] >= self.argument:
            return False
        return True


class Day(ListArgumentCriteria):
    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        if not parsed_name['date']:
            return False

        # ignore any day besides the requested on
        if parsed_name['date'].day in self.argument:
            return True
        return False


class ExceptDay(Day):
    """
    Check the day of the item is not in the list
    """
    criteria_name = "except_day"

    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)
        meets_day = super(ExceptDay, self).test(filename, parsed_name)
        if meets_day:
            return False
        return True


class HasDate(BaseCriteria):
    """
    This criteria determines if a file object has a name that
    contains a date. It is assumed to be 'on' by default, even if the
    call does not contain it explicitly.

    If this is set, but is set to False, then all items in the list will
    pass this criteria.
    """
    criteria_name = "has_date"

    def __init__(self, **kwargs):
        super(HasDate, self).__init__(kwargs)
        self.argument = True

    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)

        has_date = self.argument
        if 'date' not in parsed_name:
            parsed_name['date'] = None

        # determine if the item DOES have a date (and it should)
        if has_date and not parsed_name['date']:
            return False

        # all other cases should pass
        return True


class Hour(ListArgumentCriteria):
    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)
        self.argument = self.make_list(self.argument)
        # ignore any hour besides the requested one
        if parsed_name['date'].hour not in self.argument:
            return False
        return True


class ExceptHour(Hour):
    criteria_name = "except_hour"

    def test(self, filename, parsed_name):
        return not super(ExceptHour, self).test(filename, parsed_name)


class Year(ListArgumentCriteria):
    def test(self, filename, parsed_name):
        if not parsed_name['date']:
            return False
        if parsed_name['date'].year in self.argument:
            return True
        return False


class ExceptYear(Year):
    criteria_name = "except_year"

    def test(self, filename, parsed_name):
        return not super(ExceptYear, self).test(filename, parsed_name)

# ---------------------------------------------------------------------
# OTHER CRITERIA
# ---------------------------------------------------------------------


class Pattern(BaseCriteria):
    """
    Match against a RegExp pattern
    """
    def test(self, filename, parsed_name):
        self.debugArguments(filename, parsed_name)
        if not re.match(self.argument, filename):
            return False
        return True


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
