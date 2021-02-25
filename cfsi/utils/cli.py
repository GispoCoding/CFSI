import argparse
from collections import namedtuple
from datetime import datetime
from typing import List, Optional

Action = namedtuple("Action", "name description excludes")
ActionMap = namedtuple("ActionMap", "action method")

CLI_ACTIONS = (
    Action("build", "Rebuilds CFSI Docker images", [False]),
    Action("start", "Starts ODC database container", [False]),
    Action("init", "Initialize ODC database schema", [False]),
    Action("stop", "Stops ODC database container", [False]),
    Action("clean", "Stops ODC database container and deletes data", [False]),
    Action("index", "Index S2 images to ODC from AWS S3", [False]),
    Action("mask", "Generate cloud and shadow masks", [False]),
    Action("mosaic", "Create cloudless mosaics", [False]),
    Action("deploy", "Deploy CFSI with TerraForm", [False]),
    Action("destroy", "Destroy CFSI resources with TerraForm", [False])
)


def generate_container_name(name):
    return f"{name}_{datetime.now().strftime('%y-%m-%d_%M-%S')}"


class CFSICLIParser:
    def __init__(self):
        pass

    def parse(self) -> (List[Action], argparse.Namespace):
        """ Parses and validates user input arguments
        :return: List of methods to run and optional arguments """
        parser = self.__create_arg_parser()
        args = parser.parse_args()

        self.__validate_args(args)
        actions = self.actions_from_names(args.actions)

        vars(args).pop("actions")
        return actions, args

    @staticmethod
    def actions_from_names(action_names: List[str]) -> List[Action]:
        """ Returns a list of actions matching a list of strings """
        return [CFSICLIParser.action_from_name(name) for name in action_names]

    @staticmethod
    def action_from_name(action_name: str) -> Optional[Action]:
        """ Returns action with matching name """
        for action in CLI_ACTIONS:
            if action_name == action.name:
                return action
        return None

    @staticmethod
    def __create_arg_parser():
        parser = argparse.ArgumentParser(description="CFSI docker-compose CLI",
                                         formatter_class=argparse.RawTextHelpFormatter)

        actions_help = ",\n".join([
            f"{action.name}: {action.description}"
            for action in CLI_ACTIONS])

        parser.add_argument("actions", type=str, help=actions_help, nargs="+")
        parser.add_argument("-d", "--detach", action="store_true",
                            help=("Detach from running container,\n"
                                  "only works when a single action is given."))

        return parser

    def __validate_args(self, args: argparse.Namespace):
        if args.detach and len(args.actions) > 1:
            raise ValueError("Optional argument detach only works when"
                             "a single action argument is given!")

        actions = CFSICLIParser.actions_from_names(args.actions)

        if None in actions:
            invalid_arguments = [args.actions[i]
                                 for i, action in enumerate(actions) if not action]
            raise ValueError(f"Invalid action(s): {', '.join(invalid_arguments)}")

        self.__check_action_combination_valid(actions)

    @staticmethod
    def __check_action_combination_valid(actions: List[Action]):
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
