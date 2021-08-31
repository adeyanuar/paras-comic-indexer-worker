require('dotenv').config()
const amqp = require('amqplib')
const Database = require('../helpers/Database')
const worker = require('../workerV3')

const QUEUE_NAME = process.env.QUEUE_NAME

const sleep = (n) => {
	return new Promise((resolve) => {
		setTimeout(() => {
			resolve()
		}, n)
	})
}

const createType = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 100605,
		datetime: '2021-08-30T16:43:35.394426387+00:00',
		event_type: 'nft_create_series',
		params: {
			token_series_id: '1',
			token_metadata: {
				title: 'Naruto Shippuden ch.2: Menolong sasuke',
				description: null,
				media: 'bafybeidzcan4nzcz7sczs4yzyxly4galgygnbjewipj6haco4kffoqpkiy',
				media_hash: null,
				copies: 100,
				issued_at: null,
				expires_at: null,
				starts_at: null,
				updated_at: null,
				extra: null,
				reference:
					'bafybeicg4ss7qh5odijfn2eogizuxkrdh3zlv4eftcmgnljwu7dm64uwji',
				reference_hash: null,
			},
			creator_id: 'alice.test.near',
			price: '1000000000000000000000000',
			royalty: { 'alice.test.near': 1000 },
		},
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const payload = data.params
	const result = await db.root.collection('token_series').findOne({
		token_series_id: payload.token_series_id,
	})
	if (result) {
		console.log(`\x1b[32m`, `create_series success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `create_series failed`, `\x1b[0m`)
	}
}

const mint = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 101050,
		datetime: '2021-08-30T16:48:05.930941028+00:00',
		event_type: 'nft_transfer',
		params: { token_id: '1:1', sender_id: '', receiver_id: 'comic.test.near' },
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const payload = data.params
	const result = await db.root.collection('tokens').findOne({
		token_id: payload.token_id,
		owner_id: data.params.receiver_id,
	})
	if (result) {
		console.log(`\x1b[32m`, `mint success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `mint failed`, `\x1b[0m`)
	}
}

const transfer = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 101348,
		datetime: '2021-08-30T16:51:07.269463547+00:00',
		event_type: 'nft_transfer',
		params: {
			token_id: '1:1',
			sender_id: 'comic.test.near',
			receiver_id: 'comic1.test.near',
		},
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const payload = data.params
	const result = await db.root.collection('tokens').findOne({
		token_id: payload.token_id,
		owner_id: data.params.receiver_id,
	})
	if (result) {
		console.log(`\x1b[32m`, `transfer success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `transfer failed`, `\x1b[0m`)
	}
}

const burn = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 101148,
		datetime: '2021-08-30T16:49:05.630943005+00:00',
		event_type: 'nft_transfer',
		params: { token_id: '1:1', sender_id: 'comic1.test.near', receiver_id: '' },
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const payload = data.params
	const result = await db.root.collection('tokens').findOne({
		token_id: payload.token_id,
	})
	if (result && result.owner_id === null) {
		console.log(`\x1b[32m`, `burn success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `burn failed`, `\x1b[0m`)
	}
}

const setSeriesPrice = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 100745,
		datetime: '2021-08-30T16:45:00.522770328+00:00',
		event_type: 'nft_set_series_price',
		params: { token_series_id: '1', price: '2000000000000000000000000' },
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const result = await db.root.collection('token_series').findOne({
		token_id: data.params.token_series_id,
	})
	if (result && result.price === data.params.price) {
		console.log(`\x1b[32m`, `setSeriesPrice success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `setSeriesPrice failed`, `\x1b[0m`)
	}
}

const setSeriesNonMintable = async (channel, db) => {
	const data = {
		contract_id: 'comic.test.near',
		block_height: 101244,
		datetime: '2021-08-30T16:50:03.923371541+00:00',
		event_type: 'nft_set_series_non_mintable',
		params: { token_series_id: '1' },
	}

	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(500)
	const result = await db.root.collection('token_series').findOne({
		token_id: data.params.token_series_id,
	})
	if (result && result.price === null && result.is_non_mintable === true) {
		console.log(`\x1b[32m`, `setSeriesNonMintable success`, `\x1b[0m`)
	} else {
		console.log(`\x1b[31m`, `setSeriesNonMintable failed`, `\x1b[0m`)
	}
}

async function main() {
	const db = new Database()
	await db.init()
	const conn = await amqp.connect(process.env.AMQP_URL)

	const channel = await conn.createChannel()

	await channel.assertQueue(QUEUE_NAME, {
		durable: true,
	})

	await createType(channel, db)
	await mint(channel, db)
	await transfer(channel, db)
	await burn(channel, db)
	await setSeriesPrice(channel, db)

	await db.root.dropDatabase(process.env.DB_NAME)
	setTimeout(async () => {
		process.exit(0)
	}, 1000)
}

worker()
main()
