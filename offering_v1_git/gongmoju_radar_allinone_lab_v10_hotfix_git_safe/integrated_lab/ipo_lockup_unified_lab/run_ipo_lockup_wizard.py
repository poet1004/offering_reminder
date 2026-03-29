try:
    from .run_lockup_lab_wizard import menu
except ImportError:  # script execution
    from run_lockup_lab_wizard import menu

if __name__ == "__main__":
    menu()
