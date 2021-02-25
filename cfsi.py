#!/usr/bin/python
""" Simple CLI for CFSI. Requires Docker and Docker-Compose """

import argparse
import subprocess
from collections import namedtuple
from datetime import datetime
from typing import Callable, List, Optional


Action = namedtuple("Action", "name description method excludes")


# noinspection PyPep8Naming
class CFSI_CLI:

    def __init__(self):
        self.__actions = (
            Action("build", "Rebuilds CFSI Docker images", self.__build, [False]),
            Action("start", "Starts ODC database container", self.__start, [False]),
            Action("init", "Initialize ODC database schema", self.__initialize, [False]),
            Action("stop", "Stops ODC database container", self.__stop, [False]),
            Action("clean", "Stops ODC database container and deletes data", self.__clean, [False]),
            Action("index", "Index S2 images to ODC from AWS S3", self.__index, [False]),
            Action("mask", "Generate cloud and shadow masks", self.__mask, [False]),
            Action("mosaic", "Create cloudless mosaics", self.__mosaic, [False]))

        methods, self.__optional_args = self.__handle_args()
        print(self.__optional_args)
        self.__run_methods(methods)
        exit(0)

    def __handle_args(self) -> (List[Callable], argparse.Namespace):
        """ Parses and validates user input arguments
        :return: List of methods to run and optional arguments """
        parser = self.__create_arg_parser()
        args = parser.parse_args()

        self.__validate_args(args)
        actions = self.__actions_from_names(args.actions)
        methods = [action.method for action in actions]

        vars(args).pop("actions")
        return methods, args

    def __create_arg_parser(self):
        parser = argparse.ArgumentParser(description="CFSI docker-compose CLI",
                                         formatter_class=argparse.RawTextHelpFormatter)

        actions_help = ",\n".join([
            f"{action.name}: {action.description}"
            for action in self.__actions])

        parser.add_argument("actions", type=str, help=actions_help, nargs="+")
        parser.add_argument("-d", "--detach", action="store_true",
                            help=("Detach from running container,\n"
                                  "only works when a single action is given."))

        return parser

    def __validate_args(self, args: argparse.Namespace):
        if args.detach and len(args.actions) > 1:
            raise ValueError("Optional argument detach only works when" 
                             "a single action argument is given!")

        actions = self.__actions_from_names(args.actions)

        if None in actions:
            invalid_arguments = [args.actions[i]
                                 for i, action in enumerate(actions) if not action]
            raise ValueError(f"Invalid action(s): {', '.join(invalid_arguments)}")

        self.__check_action_combination_valid(actions)

    def __check_action_combination_valid(self, actions: List[Action]):
        conflicting_actions = []
        action_names = [action.name for action in actions]

        for action in actions:
            for excluded_action in action.excludes:
                if excluded_action in action_names:
                    conflicting_actions.append((action.name, excluded_action))

        if conflicting_actions:
            conflicting_pairs = ', '.join([f"{action[0]}-{action[1]}"
                                           for action in conflicting_actions])
            raise ValueError(f"The following pair(s) of actions cause conflicts: {conflicting_pairs}")

    def __actions_from_names(self, action_names: List[str]) -> List[Action]:
        """ Returns a list of actions matching a list of strings """
        return [self.__action_from_name(name) for name in action_names]

    def __action_from_name(self, action_name: str) -> Optional[Action]:
        """ Returns action with matching name """
        for action in self.__actions:
            if action_name == action.name:
                return action
        return None

    def __args_to_methods(self, args: argparse.Namespace) -> List[Callable]:
        """ Get list of methods to run for given arguments """
        return [self.__arg_to_method(action) for action in args.actions]

    def __arg_to_method(self, arg_action: str) -> Optional[Callable]:
        """ Get method to run for given argument """
        for action in self.__actions:
            if action.name == arg_action:
                return action.method
        return None

    @staticmethod
    def __run_methods(methods: List[Callable]):
        try:
            for method in methods:
                method()

        except Exception as err:  # TODO: custom exceptions
            raise err

    def __build(self):
        self.__run_command("docker-compose", "build", "--no-cache")

    def __start(self):
        self.__run_command("docker-compose", "up", "-d", "db")

    def __initialize(self):
        name = self.__generate_container_name("CFSI-init")
        self.__run_command("docker-compose", "run", "--name", name,
                           "odc", "cfsi/scripts/setup/setup_odc.sh")

    def __stop(self):
        self.__run_command("docker-compose", "down")

    def __clean(self):
        self.__run_command("docker-compose", "down", "--volumes")

    def __index(self):
        name = self.__generate_container_name("CFSI-index")
        self.__run_command("docker-compose", "run", "--name", name,
                           "odc", "python3", "-m", "cfsi.scripts.index.s2_index")

    def __mask(self):
        pass

    def __mosaic(self):
        pass

    @staticmethod
    def __run_command(*command: str):
        try:
            command_list = [part for part in command]
            subprocess.run(command_list, check=True)
        except subprocess.CalledProcessError as err:
            print('\n', err)

    @staticmethod
    def __generate_container_name(name):
        return f"{name}_{datetime.now().strftime('%y-%m-%d_%M-%S')}"


if __name__ == "__main__":
    try:
        CFSI_CLI()
        exit(0)
    except Exception as error:
        print("Unhandled exception: ", error)
        exit(1)
