import argparse
import config
from datetime import date, datetime


#################################################
##                                             ##
##          Input validation Functions         ##
##                                             ##
#################################################


def get_validated_date_input(prompt_message: str) -> date:
    """Prompts a user for a date and validates its YYYY-MM-DD format.

    Args:
        prompt_message (str): The message to display to the user.

    Returns:
        date: A validated date object created from the user's input.
    """
    while True:
        date_str = input(prompt_message)
        try:
            valid_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            return valid_date
        except ValueError:
            print("Invalid format. Please use YYYY-MM-DD format.")


def get_confirmation_input(prompt_message: str) -> bool:
    """Prompts the user for a yes/no confirmation.
    Args:
        prompt_message (str): The question to ask the user.
    Returns:
        bool: True if the user confirms (yes/y), False otherwise (no/n).
    """
    while True:
        response = input(prompt_message).lower().strip()
        if response in ["yes", "y"]:
            return True
        elif response in ["no", "n"]:
            return False
        else:
            print("Invalid input. Please enter 'yes' or 'no'.")

def valid_date_type(date_str: str) -> date:
    """A custom argparse type for validating 'YYYY-MM-DD' date strings.

    Args:
        date_str (str): The date string from the command-line argument.

    Returns:
        date: The validated date object if the format is correct.

    Raises:
        argparse.ArgumentTypeError: If the string is not a valid date in the
            'YYYY-MM-DD' format.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"'{date_str}' is not a valid date. Use YYYY-MM-DD."
        )
    
