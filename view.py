import sys
import re
import datetime
import storage_transaction
import numpy as np

class View:

    def __init__(self):
        self.bitbay = storage_transaction.StorageTransaction("https://bitbay.net/API/Public/BTCPLN/trades.json", 'bitbay')
        self.bitmarket = storage_transaction.StorageTransaction("https://www.bitmarket.pl/json/BTCPLN/trades.json",
                                                                'bitmarket', count=1000)

    def get_transactions(self, date):
        """Find transaction in bitbay and bimtmarket, format input('%Y-%m-%d:%H:%M')
        
        :param date:datetime time transaction which user wants to get 
        :return: 
        """
        if self.check_format(date):
            date = self.extract_data(date)
            bitbay_transaction = self.bitbay.gradient_search_transactions(date)
            if len(bitbay_transaction) > 0:
                bitbay_transaction = self.average_batch_transaction(bitbay_transaction)
            bitmarket_transaction = self.bitmarket.gradient_search_transactions(date)
            if len(bitmarket_transaction) > 0:
                bitmarket_transaction = self.average_batch_transaction(bitmarket_transaction)

            self.print_results(bitmarket_transaction, bitbay_transaction)
            self.bitmarket.save_transactions()
            self.bitbay.save_transactions()


    def print_results(self, bitmarket_transaction, bitbay_transaction):
        print('bitbay: {:0.2f} PLN'.format(float(bitbay_transaction)))
        print('bitmarket: {:0.2f} PLN'.format(float(bitmarket_transaction)))


    def extract_data(self, date):
        """Convert string into datatime"""
        return datetime.datetime.strptime(date, '%Y-%m-%d:%H:%M')

    def average_batch_transaction(self, transaction):
        """Convert batch of transactions into one value
        
        :param transaction:DataFrame, conatins transaction in the same time
        :return: 
        """
        transaction_list = list(transaction['price'])
        return np.average(transaction_list)


    def check_format(self, date):
        try:
            datetime.datetime.strptime(date, '%Y-%m-%d:%H:%M')
            return True
        except ValueError:
            return False

if __name__ == '__main__':
    view = View()
    date = sys.argv[1]
    view.get_transactions(date)