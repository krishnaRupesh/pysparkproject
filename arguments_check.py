import ast
import argparse


class DailyTxnReport():

    def __init__(self, txn_hudi_base_path, txn_oas_hudi_base_path,
                 txn_tax_hudi_base_path, run_date, sdb_path, loc_customers, temp_path):
        self.txn_hudi_base_path = txn_hudi_base_path
        self.txn_oas_hudi_base_path = txn_oas_hudi_base_path
        self.txn_tax_hudi_base_path = txn_tax_hudi_base_path
        self.run_date = run_date
        self.sdb_path = sdb_path
        self.loc_customers = loc_customers
        print(loc_customers)
        print(type(loc_customers))


class SourceURIAction(argparse.Action):
    """Custom Validation."""

    def __call__(self, parser, namespace, value, option_string=None):
        """Custom implementation."""
        setattr(namespace, self.dest, ast.literal_eval(value))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--txn_hudi_base_path', required=True, type=str)
    parser.add_argument(
        '--txn_oas_hudi_base_path', required=True, type=str)
    parser.add_argument(
        '--txn_tax_hudi_base_path', required=True, type=str)
    parser.add_argument(
        '--run_date', required=True, type=str)
    parser.add_argument(
        '--merchant_sdb_path', required=True, type=str)
    parser.add_argument(
        '--loc_customers', required=True, type=str, action=SourceURIAction)
    parser.add_argument(
        '--temp_path', required=True, type=str)

    args = parser.parse_args()
    print(args)

    cs_obj = DailyTxnReport(
        args.txn_hudi_base_path,
        args.txn_oas_hudi_base_path,
        args.txn_tax_hudi_base_path,
        args.run_date,
        args.merchant_sdb_path,
        args.loc_customers,
        args.temp_path

    )


