const { MongoClient, Logger } = require('mongodb')

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
}

module.exports = Database
