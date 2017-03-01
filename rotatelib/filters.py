import re
import collections
import datetime
import inspect

class BaseFilter(object):
    filter_name = None

    def __init__(self, debug=False):
        self.debugMode = False

    def debug(self, message):
        if not self.debugMode:
            return
        print(message)

    def debugArguments(self, filename, parsed_name):
        if not self.debugMode:
            return

    def filter(self, items):
        return_items = []
        for item in items:
            return_items.append(item['item'])
        return return_items

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


class ExceptFirst(BaseFilter):
    filter_name = 'except_first'

    def __init__(self, debug=False):
        super(ExceptFirst, self).__init__(debug)
        self.dates = {}
        self.reverse = True

    def filter(self, items):
        return_items = []

        # for each item, we want to add it to our self.dates structure by
        # date so we can sort them later
        for item in items:
            if self.argument == 'month':
              date_string = "%d%d" % (item['parsed']['date'].year, item['parsed']['date'].month)
            else:
              date_string = "%d%d%d" % (item['parsed']['date'].year, item['parsed']['date'].month, item['parsed']['date'].day)

            if date_string not in self.dates:
              self.dates[date_string] = []
            self.dates[date_string].append(item)

        for date in self.dates:
            if len(self.dates[date]) == 1:
                # there is only one item, we don't need to add it
                pass
            else:
                # there is more than one value in this day, so we want to sort them by date first
                self.dates[date].sort(key=lambda x: x['parsed']['date'], reverse=self.reverse)

                # next remove the last item (in this case, the last item is the "first")
                self.dates[date].pop()

                # add the rest of values to the list
                return_items.extend([item['item'] for item in self.dates[date]])

        return return_items

class ExceptLast(ExceptFirst):
    filter_name = 'except_last'

    def __init__(self, debug=False):
        super(ExceptLast, self).__init__(debug)
        self.reverse = False
