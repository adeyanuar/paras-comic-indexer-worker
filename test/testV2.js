require('dotenv').config()
const amqp = require('amqplib')
const Database = require('../helpers/Database')
const worker = require('../workerV2')

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
		contract_id: 'comic6.test.near',
		datetime: new Date().getTime(),
		event_type: 'nft_create_series',
		params: {
			token_series_id: '373',
			token_metadata: {
				title: 'Coin',
				description: null,
				media: 'QmRN7WTEPwn1TaueZUkWkcubmRYDnNMyKUBL69hdBNzNPV',
				media_hash: null,
				copies: null,
				issued_at: null,
				expires_at: null,
				starts_at: null,
				updated_at: null,
				extra: null,
				reference:
					'bafybeibcb6yfzcutu4caihsyjfujdd2makxsrids47kf2jw6rs3ywdtjem',
				reference_hash: null,
			},
			creator_id: 'awra.near',
			price: null,
			royalty: {},
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
		contract_id: 'comic6.test.near',
		datetime: new Date().getTime(),
		event_type: 'nft_transfer',
		params: { token_id: '373:120', sender_id: '', receiver_id: 'iis.near' },
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
		contract_id: 'comic6.test.near',
		datetime: new Date().getTime(),
		event_type: 'nft_transfer',
		params: {
			token_id: '373:120',
			sender_id: 'iis.near',
			receiver_id: 'bambang.near',
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
		contract_id: 'comic6.test.near',
		datetime: new Date().getTime(),
		event_type: 'nft_transfer',
		params: {
			token_id: '373:120',
			sender_id: 'bambang.near',
			receiver_id: '',
		},
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

// const mint = async (channel, db) => {
// 	const data = {
// 		contract_id: 'comic6.test.near',
// 		event_type: 'mint',
// 		params: {
// 			token_id: 'naruto-8:13',
// 			metadata: {
// 				title: 'Naruto Shippuden ch.2: Menolong sasuke #13',
// 				description: null,
// 				media: 'bafybeiax25bdn5go7b6xoc7kocfw3kbzng7ijnpemdo6aiogumzx53s6ga',
// 				media_hash: null,
// 				copies: null,
// 				issued_at: null,
// 				expires_at: null,
// 				starts_at: null,
// 				updated_at: null,
// 				extra: null,
// 				reference:
// 					'bafybeihvoofv5rkclwipij3rlozllrpyuc4wpcux6jve6o6qdbm4v7gepi',
// 				reference_hash: null,
// 			},
// 			owner_id: 'comic6.test.near',
// 		},
// 	}
// 	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
// 	await sleep(250)
// 	const payload = data.params
// 	const [comic_id, chapter_id] = payload.token_id.split('-')
// 	const result = await db.root.collection('tokens').findOne({
// 		token_id: payload.token_id,
// 		comic_id: comic_id,
// 		chapter_id: parseInt(chapter_id),
// 	})
// 	if (result?.metadata.title === payload.metadata.title) {
// 		console.log('succeed')
// 	} else {
// 		console.log('failed')
// 	}
// }

// const transfer = async (channel, db) => {
// 	const data = {
// 		contract_id: 'comic6.test.near',
// 		event_type: 'transfer',
// 		params: {
// 			token_id: 'naruto-8:13',
// 			sender_id: 'comic6.test.near',
// 			receiver_id: 'comic5.test.near',
// 		},
// 	}
// 	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
// 	await sleep(1000)
// 	const payload = data.params
// 	const result = await db.root.collection('tokens').findOne({
// 		token_id: payload.token_id,
// 	})
// 	const senderAccess = await db.root.collection('access').findOne({
// 		account_id: payload.sender_id,
// 	})
// 	const receiverAccess = await db.root.collection('access').findOne({
// 		account_id: payload.receiver_id,
// 	})
// 	if (
// 		result?.owner_id === payload.receiver_id &&
// 		!senderAccess.access_tokens.includes(payload.token_id) &&
// 		receiverAccess.access_tokens.includes(payload.token_id)
// 	) {
// 		console.log('succeed')
// 	} else {
// 		console.log('failed')
// 	}
// }

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

	await db.root.dropDatabase(process.env.DB_NAME)
	setTimeout(async () => {
		process.exit(0)
	}, 1000)
}

worker()
main()
