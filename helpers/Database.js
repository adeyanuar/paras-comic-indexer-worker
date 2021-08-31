const { MongoClient, Logger, Decimal128 } = require('mongodb')

class Database {
	constructor() {
		this.ready = null
		this.client = new MongoClient(
			`${process.env.MONGO_URL}?retryWrites=true&w=majority`,
			{ useNewUrlParser: true, useUnifiedTopology: true }
		)
	}

	async init() {
		try {
			await this.client.connect()
			this.root = this.client.db(process.env.DB_NAME)

			if (process.env.NODE_ENV === 'development') {
				Logger.setLevel('debug')
			}
			this.ready = true
		} catch (err) {
			console.log(err)
		}
	}

	toDecimal128(x) {
		return Decimal128.fromString(x)
	}
}

module.exports = Database
