import requests
import json
import pandas as pd
import datetime


class StorageTransaction:

    def __init__(self, url, filename, count=100):
        self.url = url
        self.transactions = None
        self.count_transaction = count
        self.counter_delta_trans = count//2 - 10
        self.filename= filename
        self.read_transaction_from_csv()
        self.download_first_trades()

    def download_first_trades(self):
        """Download some initial trades
        
        :return: 
        """
        newest_tid, response_length = self.get_newest_tid_trans()

        for i in range(newest_tid - self.count_transaction, newest_tid, response_length):
            url_pattern = "{}?since={}".format(self.url, i)
            request_content = requests.get(url_pattern)
            self.procces_downloaded_data(request_content)

    def procces_downloaded_data(self, data):
        """Transform response from market API into DataFrame where 'tid' is index column
        
        :param data:str response from market API contains trades
        :return: 
        """
        data = json.loads(data.content)
        data = pd.DataFrame(data)
        fun = lambda x: datetime.datetime.fromtimestamp(x)
        data['date'] = data['date'].apply(fun)
        data['tid'] = data['tid'].apply(lambda x: int(x))
        data.set_index('tid', inplace=True)
        if self.transactions is None:
            self.transactions = data
        else:
            self.transactions = self.transactions.combine_first(data)

    def gradient_search_transactions(self, date):
        """This function try to find transaction at the given time. If this transaction doesn't exist, it try to predict 
        the 'tid' of given transaction and download it
        
        :param date:datetime time transaction which user wants to get 
        :return: list contains transaction at the given time(includes different seconds)
        """
        transaction = self.check_date(date)
        step_factor = 1.0
        self.old_nearest_date = date
        while transaction is None:
            narest_date_index, iterate_direction = self.find_nearest_date(date)
            print('\r', end='')
            print('Downloaded records: {0}, nearest date: {1}'.format(self.transactions.shape[0],
                                                                      self.transactions.loc[narest_date_index]['date']),
                  end='\t', flush=True)

            if self.old_nearest_date != self.transactions.loc[narest_date_index]['date']:
                step_factor = 1.0
            time_delta = self.count_time_delta(narest_date_index, iterate_direction)
            index = self.get_probable_index(date, narest_date_index, time_delta, step_factor)
            for i in range(2):
                transaction = self.get_transactions_by_index(index + i * self.count_transaction//2)
                self.procces_downloaded_data(transaction)

            transaction = self.check_date(date)
            step_factor -= 0.05
        print('\n')
        return transaction

    def get_probable_index(self, goal_date, nearest_date_index, time_delta, step_factor):
        """This function count the most probable position of wanted transaction
        
        :param goal_date: datetime time transaction which user wants to get 
        :param nearest_date_index: index of the nearest index of date
        :param time_delta: local segment time (a kind of differential) 
        :param step_factor: change working of algorithm, sometimes the algorithm falls into the local minimum 
        :return: int, probable 'tid' number of wanted transaction
        """
        nearest_date = self.transactions.loc[nearest_date_index]['date']

        goal_index = (goal_date - nearest_date)/abs(time_delta) * step_factor
        index = max(0, int(nearest_date_index + goal_index - self.count_transaction/2))
        return index


    def check_date(self, date):
        """This function try to find transaction at the given time.
         Sometimes transaction has never exist at the given, so this function also check it and return nearest transaction
        
        :param date:datetime time transaction which user wants to get 
        :return: list contains transaction at the given time(includes different seconds)
        """
        equal_data = self.transactions[self.transactions['date'].apply(lambda x: x.date() == date.date())]
        if len(equal_data) == 0:
            return None
        equal_hour = equal_data[equal_data['date'].apply(lambda x: x.hour == date.hour)]
        if len(equal_hour) == 0:
            if self.check_neighbours(equal_data, date):
                return equal_data
            return None
        equal_minute = equal_hour[equal_hour['date'].apply(lambda x: x.minute == date.minute)]
        if len(equal_minute) == 0:
            if self.check_neighbours(equal_hour, date):
                return equal_hour
            return None
        return equal_minute

    def check_neighbours(self, set_of_dates, date):
        """This function check if date of transaction exists in market  
        
        :param set_of_dates: batch of transaction which are nearest the wanted date
        :param date:datetime time transaction which user wants to get 
        :return: boolean, info about if date exists
        """
        lower = set_of_dates[set_of_dates['date'] < date]
        grater = set_of_dates[set_of_dates['date'] > date]
        if len(lower) == 0 or len(grater) == 0:
            return False

        lower_max_index = lower['date'].idxmax()
        grater_min_index = grater['date'].idxmin()
        return grater_min_index - lower_max_index == 1

    def find_nearest_date(self, date):
        """This function find nearest date to date which user want to get
        
        :param date:datetime time transaction which user wants to get 
        :return: datetime, contains nearest date to date which user want to get
        """
        lower = self.transactions[self.transactions['date'] < date]
        grater = self.transactions[self.transactions['date'] > date]
        if len(lower) == 0:
            return grater['date'].idxmin(), False
        elif len(grater) == 0:
            return lower['date'].idxmax(), True

        lower_max_value = lower.loc[lower['date'].idxmax()]['date']
        grater_min_value = grater.loc[grater['date'].idxmin()]['date']
        if date - lower_max_value <= grater_min_value - date:
            return lower['date'].idxmax(), True
        else:
            return grater['date'].idxmin(), False

    def count_time_delta(self, index, iterate_direction):
        """This function take some transaction which are close to index and then calculate local average time delta
        
        :param index:int, 'tid' of transaction
        :param iterate_direction: boolean, inform about gradient direction
        :return:average_timedelta float,  
        """
        if iterate_direction:
            list_time = list(self.transactions.sort_index().loc[index - self.counter_delta_trans:index]['date'])
        else:
            list_time = list(self.transactions.sort_index().loc[index:index + self.counter_delta_trans]['date'])
        self.old_nearest_date = self.transactions.loc[index]['date']
        delta_list = []
        for a, b in zip(list_time[:-1], list_time[1:]):
            delta_list.append(a - b)
        average_timedelta = sum(delta_list, datetime.timedelta(0)) / len(delta_list)
        return average_timedelta

    def get_transactions_by_index(self, index):
        """Send http request to get batch of transaction by 'tid'
        
        :param index:int 'tid' of transaction
        :return: response from http request 
        """
        url = "{}?{}{}".format(self.url, "since=", index)
        request_content = requests.get(url)
        if request_content.status_code == 200:
            return request_content

    def get_newest_tid_trans(self):
        """Download newet 'tid
        
        :return:newest_tid int, contains index of newest transaction
        response_length int, inform about number of received transaction in one http response
        """
        url = "{}?{}".format(self.url, "sort=desc")
        request_content = requests.get(url)
        newest_tid = -1
        response_length = 0
        if request_content.status_code == 200:
            transactions = request_content.content
            data = json.loads(transactions)
            data = pd.DataFrame(data)
            response_length = len(data)
            newest_tid = data["tid"].max()
        return int(newest_tid), response_length

    def read_transaction_from_csv(self):
        filename = '{}.csv'.format(self.filename)
        # try:
        data = pd.read_csv(filename)
        data['date'] = data['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
        data['tid'] = data['tid'].apply(lambda x: int(x))
        data.set_index('tid', inplace=True)
        self.transactions = data
        # except Exception:
        #     pass

    def save_transactions(self,):
        self.transactions.to_csv('{}.csv'.format(self.filename))

if __name__ == "__main__":

    x = StorageTransaction("https://bitbay.net/API/Public/BTCPLN/trades.json")
    s = x.gradient_search_transactions(datetime.datetime(2015, 3, 10, 2, 4))

