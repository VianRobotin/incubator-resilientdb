#!/usr/bin/env python3

from __future__ import print_function
import util
import argparse
import os
import sys
import time

DEFAULT_NUM_MACHINES=1
DEFAULT_TIME="0:15:00"
DEFAULT_TIMEOUT=1800

class InvalidNumMachinesException(Exception): pass
class ReservationFailedException(Exception): pass
class ReservationNotFoundException(Exception): pass

class PreserveReservation:
    def __init__(self, reservation_id, username, start_time, end_time, state, num_machines, assigned_machines):
        self.__reservation_id = reservation_id
        self.__username = username
        self.__start_time = start_time
        self.__end_time = end_time
        self.__state = state
        self.__num_machines = num_machines
        self.__assigned_machines = assigned_machines

    @property
    def reservation_id(self):
        return self.__reservation_id

    @property
    def username(self):
        return self.__username

    @property
    def start_time(self):
        return self.__start_time

    @property
    def end_time(self):
        return self.__end_time

    @property
    def state(self):
        return self.__state

    @property
    def num_machines(self):
        return self.__num_machines

    @property
    def assigned_machines(self):
        return self.__assigned_machines

def _parse_int_or_default(value, default_value):
    try:
        return int(value)
    except ValueError:
        return default_value

def PreserveReservation_from_preserve_line(line):
    parts = line.split()
    return PreserveReservation(
            reservation_id=int(parts[0]),
            username=parts[1],
            start_time="%s %s" % (parts[2], parts[3]),
            end_time="%s %s" % (parts[4], parts[5]),
            state=parts[6],
            num_machines=_parse_int_or_default(parts[7], 0),
            assigned_machines=sorted(["%s" % part for part in parts[8:]])
        )

class PreserveManager:
    def __init__(self, username):
        self.__username = username

    @property
    def username(self):
        return self.__username

    def get_reservations(self):
        list_output = util.execute_command_for_output(["preserve", "-llist"])
        reservations = {}
        found_header = False
        for line in list_output.split('\n'):
            if not found_header:
                if line.startswith("id"):
                    found_header = True
            elif line.strip():
                reservation = PreserveReservation_from_preserve_line(line)
                reservations[reservation.reservation_id] = reservation
        return reservations

    def get_own_reservations(self):
        reservations = self.get_reservations()
        return { k: v for k, v in reservations.items() if v.username == self.username }

    def create_reservation(self, num_machines, time):
        if num_machines < 1:
            raise InvalidNumMachinesException("Number of machines must be at least one.")

        print(f"Reserving {num_machines} machines for {time}")
        reservation_output = util.execute_command_for_output(["preserve", "-np", str(num_machines), "-t", time])

        reservation_id = None
        for line in reservation_output.split('\n'):
            if line.startswith("Reservation number"):
                reservation_id = line.strip(":").split(" ")[-1]
                break
        if reservation_id is None:
            raise ReservationFailedException("preserve did not print a reservation id. Output:\n%s" % reservation_output)

        return int(reservation_id)

    def fetch_reservation(self, reservation_id):
        if str(reservation_id).upper() == "LAST":
            reservations = self.get_own_reservations()
            if len(reservations) == 0:
                raise ReservationNotFoundException("Could not retrieve last reservation; no reservations were found.")
            reservation_ids = sorted(reservations.keys())
            return reservations[reservation_ids[-1]]
        else:
            reservations = self.get_reservations()
            if not int(reservation_id) in reservations:
                raise ReservationNotFoundException('Could not find reservation for id "%s".' % reservation_id)
            return reservations[int(reservation_id)]

    def kill_reservation(self, reservation_id):
        reservation = self.fetch_reservation(reservation_id)
        if not reservation.username == self.username:
            raise ReservationNotFoundException("Reservation for given id does not belong to the user.")
        util.execute_command_quietly(["preserve", "-c", str(reservation.reservation_id)])
