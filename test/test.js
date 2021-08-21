require('dotenv').config()
const amqp = require('amqplib')
const Database = require('../helpers/Database')
const worker = require('../worker')

const QUEUE_NAME = 'indexer'

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
		event_type: 'create_type',
		params: {
			token_type: 'naruto-9',
			token_metadata: {
				title: 'Naruto Shippuden ch.2: Menolong sasuke',
				description: null,
				media: 'bafybeiax25bdn5go7b6xoc7kocfw3kbzng7ijnpemdo6aiogumzx53s6ga',
				media_hash: null,
				copies: null,
				issued_at: null,
				expires_at: null,
				starts_at: null,
				updated_at: null,
				extra: null,
				reference:
					'bafybeihvoofv5rkclwipij3rlozllrpyuc4wpcux6jve6o6qdbm4v7gepi',
				reference_hash: null,
			},
			price: '1000000000000000000000000',
		},
	}
	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(250)
	const payload = data.params
	const [comic_id, chapter_id] = payload.token_type.split('-')
	const result = await db.root.collection('chapters').findOne({
		token_type: payload.token_type,
		comic_id: comic_id,
		chapter_id: parseInt(chapter_id),
	})
	if (result?.metadata.title === payload.token_metadata.title) {
		console.log('succeed')
	} else {
		console.log('failed')
	}
}

const mint = async (channel, db) => {
	const data = {
		contract_id: 'comic6.test.near',
		event_type: 'mint',
		params: {
			token_id: 'naruto-8:13',
			metadata: {
				title: 'Naruto Shippuden ch.2: Menolong sasuke #13',
				description: null,
				media: 'bafybeiax25bdn5go7b6xoc7kocfw3kbzng7ijnpemdo6aiogumzx53s6ga',
				media_hash: null,
				copies: null,
				issued_at: null,
				expires_at: null,
				starts_at: null,
				updated_at: null,
				extra: null,
				reference:
					'bafybeihvoofv5rkclwipij3rlozllrpyuc4wpcux6jve6o6qdbm4v7gepi',
				reference_hash: null,
			},
			owner_id: 'comic6.test.near',
		},
	}
	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(250)
	const payload = data.params
	const [comic_id, chapter_id] = payload.token_id.split('-')
	const result = await db.root.collection('tokens').findOne({
		token_id: payload.token_id,
		comic_id: comic_id,
		chapter_id: parseInt(chapter_id),
	})
	if (result?.metadata.title === payload.metadata.title) {
		console.log('succeed')
	} else {
		console.log('failed')
	}
}

const transfer = async (channel, db) => {
	const data = {
		contract_id: 'comic6.test.near',
		event_type: 'transfer',
		params: {
			token_id: 'naruto-8:13',
			sender_id: 'comic6.test.near',
			receiver_id: 'comic5.test.near',
		},
	}
	channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(data)))
	await sleep(1000)
	const payload = data.params
	const result = await db.root.collection('tokens').findOne({
		token_id: payload.token_id,
	})
	const senderAccess = await db.root.collection('access').findOne({
		account_id: payload.sender_id,
	})
	const receiverAccess = await db.root.collection('access').findOne({
		account_id: payload.receiver_id,
	})
	if (
		result?.owner_id === payload.receiver_id &&
		!senderAccess.access_tokens.includes(payload.token_id) &&
		receiverAccess.access_tokens.includes(payload.token_id)
	) {
		console.log('succeed')
	} else {
		console.log('failed')
	}
}

async function main() {
	const db = new Database()
	await db.init()
	const conn = await amqp.connect(`amqp://b:b@localhost:5672/`)

	const channel = await conn.createChannel()

	await channel.assertQueue(QUEUE_NAME, {
		durable: true,
	})

	await createType(channel, db)
	await mint(channel, db)
	await transfer(channel, db)

	await db.root.dropDatabase('comic-indexer')
	setTimeout(async () => {
		process.exit(0)
	}, 1000)
}

worker()
main()

// ;(async () => {
// 	try {
// 		const broker = await Broker.create(config)
// 		broker.on('error', console.error)

// 		// // Publish a message
// 		const publication = await broker.publish('demo_publication', 'Hello World!')
// 		publication.on('error', console.error)

// 		// Consume a message
// 		const subscription = await broker.subscribe('demo_subscription')
// 		subscription
// 			.on('message', (message, content, ackOrNack) => {
// 				console.log(content)
// 				ackOrNack()
// 			})
// 			.on('error', console.error)
// 	} catch (err) {
// 		console.error(err)
// 	}
// })()
