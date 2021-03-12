#!/usr/bin/python
""" Simple CLI for CFSI. Requires Docker and Docker-Compose """

import subprocess
from typing import Callable, Dict, List

from cfsi.utils.cli import CFSICLIParser, generate_container_name


# noinspection PyPep8Naming
class CFSI_CLI:

    def __init__(self):
        parser = CFSICLIParser()
        self.__actions, self.__optional_args = parser.parse()
        self.__action_map = self.__get_action_map()

        self.__methods = [self.__action_map[action.name] for action in self.__actions]
        self.__run()
        exit(0)

    def __get_action_map(self) -> Dict[str, Callable]:
        """ Dict that maps actions to methods """
        return {"build": self.__build, "start": self.__start, "init": self.__initialize,
                "stop": self.__stop, "clean": self.__clean, "index": self.__index,
                "mask": self.__mask, "mosaic": self.__mosaic, "deploy": self.__deploy,
                "destroy": self.__destroy, "log": self.__log}

    def __run(self):
        try:
            for method in self.__methods:
                method()

        except Exception as err:  # TODO: custom exceptions
            raise err

    def __build(self):
        self.__run_command("docker-compose", "build", "--no-cache")

    def __start(self):
        self.__run_command("docker-compose", "up", "-d", "db")

    def __initialize(self):
        name = generate_container_name("CFSI-init")
        self.__wait_for_db(name)
        command = self.__generate_compose_run_command(name) + [
            "odc", "cfsi/scripts/setup/setup_odc.sh"]
        self.__run_command(*command)

    def __stop(self):
        self.__run_command("docker-compose", "down")

    def __clean(self):
        self.__run_command("docker-compose", "down", "--volumes")

    def __index(self):
        name = generate_container_name("CFSI-index")
        self.__wait_for_db(name)
        command = self.__generate_compose_run_command(name) + [
            "odc", "python3", "-m", "cfsi.scripts.index.s2_index"]
        self.__run_command(*command)

    def __mask(self):
        name = generate_container_name("CFSI-mask")
        command = self.__generate_compose_run_command(name) + [
            "odc", "python3", "-m", "cfsi.scripts.process.create_masks"]
        self.__run_command(*command)

    def __mosaic(self):
        name = generate_container_name("CFSI-mosaic")
        command = self.__generate_compose_run_command(name) + [
            "odc", "python3", "-m", "cfsi.scripts.masks.create_mosaics"]
        self.__run_command(*command)

    def __deploy(self):
        self.__run_command("terraform", "apply")

    def __destroy(self):
        self.__run_command("terraform", "destroy")

    def __log(self):
        self.__run_command("docker-compose", "logs", "-f")

    @staticmethod
    def __run_command(*command: str):
        try:
            command_list = [part for part in command]
            subprocess.run(command_list, check=True)

        except subprocess.CalledProcessError as err:
            print('\n', err)

    def __generate_compose_run_command(self, name: str) -> List[str]:
        """ Generates a base for docker-compose run commands """
        base = ["docker-compose", "run", "--name", name, "--rm"]

        if self.__optional_args.detach and "waiter" not in name:
            base[-1] = "-d"

        return base

    def __wait_for_db(self, name: str = ""):
        if name:
            name += "_waiter"
        else:
            name = generate_container_name("CFSI-waiter")

        command = self.__generate_compose_run_command(name) + [
            "odc", "cfsi/utils/wait-for-it.sh", "db:5432"]
        self.__run_command(*command)


if __name__ == "__main__":
    try:
        CFSI_CLI()
        exit(0)

    except KeyboardInterrupt:
        print("Aborted")
        exit(1)
    except Exception as error:
        print("Unhandled exception: ", error)
        exit(1)
