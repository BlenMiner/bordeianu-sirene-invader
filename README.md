# Sirene Invader

Getting started:

- Download data inside **data/StockEtablissement_utf8.csv**

- Install dependencies
```console
npm install
```

- Execute program
```console
npm run run
```

How it works:

- It looks for *data/StockEtablissement_utf8.csv* and splits the file into chunks of around 30 MB.
- It creates parallel workers based on the CPU count to process each chunk.
- It uploads each chunk to the database based on the filter object.
- Once everything is done, it prints some analytics.

Details:

The paralel workers are a separate process that communicates via signals with the main process.
The first thing it does is notify the main process that it is ready for work (after connection to the MongoDB database).
The main process has a list of available workers and a list of queued work.
Once work is available and there is at least a free worker, a worker will be set as busy and the queued work will be removed and processed by the said worker.
Once the worker finished another signal is sent notifying the main process which it then calculated the times it took to finish and sets it as free again.
This process keeps happening until no more work is available.

Benchmark:

| CPU | RAM | Average Time (Start to Finish) | Average Per Worker |
|-----|-----|--------------------------------|--------------------|
| Intel(R) Core(TM) i7-4790K CPU @ 4.00GHz | 16.0 GB   | 5 min | 12.16 sec |

Successful run:
![](https://i.ibb.co/JsJm0FM/sirene.png)

MongoDB after run:
![](https://i.ibb.co/Lp1R9Yj/databasemongo.png)
