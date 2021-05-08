from mdt.database import load_rxnorm, load_meps


def main():
    load_rxnorm()
    load_meps()


if __name__ == '__main__':
    main()
