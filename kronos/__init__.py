from .kronos_version import kronos_version
__version__ = kronos_version

def main():
    from kronos import main as km
    km()


