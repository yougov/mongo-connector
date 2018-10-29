# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import optparse
import sys

from mongo_connector import errors
from mongo_connector.constants import __version__


def default_apply_function(option, cli_values):
    first_value = list(cli_values.values())[0]
    if first_value is not None:
        option.value = first_value


class Option(object):
    """A config file option which can be overwritten on the command line.

    config_key is the corresponding field in the JSON config file.

    apply_function has the following signature:
    def apply_function(option, cli_values):
        # modify option.value ...

    When apply_function is invoked, option.value will be set to the
    value given in the config file (or the default value).

    apply_function reads the cli_values and modifies option.value accordingly
    """

    def __init__(
        self,
        config_key=None,
        default=None,
        type=None,
        apply_function=default_apply_function,
    ):
        self.config_key = config_key
        self.value = default
        self.type = type
        self.apply_function = apply_function

        self.cli_names = []
        self.cli_options = []

    def validate_type(self):
        if self.type == str:
            return isinstance(self.value, str)
        else:
            return isinstance(self.value, self.type)

    def add_cli(self, *args, **kwargs):
        """Add a command line argument.

        All of the given arguments will be passed directly to
        optparse.OptionParser().add_option
        """
        self.cli_options.append((args, kwargs))


class Config(object):
    """Manages command line application configuration.

    conf = Config(options)
    conf.parse_args()
    value = conf['key']
    value2 = conf['key1.key2'] # same as conf['key1']['key2']
    """

    def __init__(self, options):
        self.options = options

        self.config_key_to_option = dict(
            [(option.config_key, option) for option in self.options]
        )

    def parse_args(self, argv=None):
        """Parses command line arguments from stdin (or given argv).

        Does the following:
        1. Parses command line arguments
        2. Loads config file into options (if config file specified)
        3. calls option.apply_function with the parsed cli_values
        """

        # parse the command line options
        parser = optparse.OptionParser(version="%prog version: " + __version__)
        for option in self.options:
            for args, kwargs in option.cli_options:
                cli_option = parser.add_option(*args, **kwargs)
                option.cli_names.append(cli_option.dest)
        parsed_options, args = parser.parse_args(argv)
        if args:
            raise errors.InvalidConfiguration(
                "The following command line arguments are not recognized: "
                + ", ".join(args)
            )

        # load the config file
        if parsed_options.config_file:
            try:
                with open(parsed_options.config_file) as f:
                    self.load_json(f.read())
            except (OSError, IOError, ValueError) as exc:
                tb = sys.exc_info()[2]
                raise errors.InvalidConfiguration(str(exc)).with_traceback(tb)

        # apply the command line arguments
        values = parsed_options.__dict__
        for option in self.options:
            option.apply_function(
                option, dict((k, values.get(k)) for k in option.cli_names)
            )

    def __getitem__(self, key):
        keys = key.split(".")
        cur = self.config_key_to_option[keys[0]].value
        for k in keys[1:]:
            if cur is not None:
                if isinstance(cur, dict):
                    cur = cur.get(k)
                else:
                    cur = None
        return cur

    def load_json(self, text):
        parsed_config = json.loads(text)
        for k in parsed_config:
            option = self.config_key_to_option.get(k)
            if option:
                # load into option.value
                if isinstance(parsed_config[k], dict):
                    for k2 in parsed_config[k]:
                        option.value[k2] = parsed_config[k][k2]
                else:
                    option.value = parsed_config[k]

                # type check
                if not option.validate_type():
                    raise errors.InvalidConfiguration(
                        "%s should be a %r, %r was given!"
                        % (
                            option.config_key,
                            option.type.__name__,
                            type(option.value).__name__,
                        )
                    )
            else:
                if not k.startswith("__"):
                    logging.warning("Unrecognized option: %s" % k)
