import pickle
with open('tickers.pkl', 'rb') as f:

    student_names_loaded = pickle.load(f) 
    print(student_names_loaded) 
