import sys
import argparse


def init_db(args):
    print(args)
    print('init db')


def module_create(args):
    print(args)
    print('module_create')


def module_build(args):
    print(args)
    print('module_build')


def main():
    # Main command and child command setup
    parser = argparse.ArgumentParser(
        description='Medication Diversification Tool for Synthea'
    )

    subparsers = parser.add_subparsers(
        title='Commands',
        metavar='',
    )

    # Init command parsers

    init_parser = subparsers.add_parser(
        'init',
        description='Download Meps, RxNorm data and Setup the Database',
        help='Initialize MDT DB'
    )
    init_parser.set_defaults(func=init_db)

    # Module ommand parsers

    module_parser = subparsers.add_parser(
        'module',
        description='Module specific commands',
        help='Module commands'
    )
    module_parser.add_argument(
        '--module-name',
        '-n',
        help='Specific name of ouputed module',
    )

    module_subparser = module_parser.add_subparsers(
        title='Commands',
        metavar='',
        dest='module_commands'
    )

    create_parser = module_subparser.add_parser(
        'create',
        description='Create template module directory',
        help='Create template module directory'
    )
    create_parser.set_defaults(func=module_create)

    build_parser = module_subparser.add_parser(
        'build',
        description='build synthea module',
        help='build synthea module'
    )

    build_parser.set_defaults(func=module_build)

    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    try:
        args.func(args)
    except AttributeError:
        for key, _ in vars(args).items():
            if key == 'module_commands':
                module_parser.print_help()
