# recon

Retrieves high density observations from hurricane reconnaissance aircraft and inserts into a Postgres database.

You can read more [here](https://www.nhc.noaa.gov/abouthdobs_2007.shtml).

## Install

Configure Postgres connection in `lib/models.py`. For example,

`engine = create_engine("postgresql://postgres:dev@localhost:5432/postgres")`

## Usage

Run `main.py` to collect and insert the data.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://github.com/smehlhoff/recon/blob/master/LICENSE)
