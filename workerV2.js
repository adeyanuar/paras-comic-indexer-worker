require('dotenv').config()
const amqp = require('amqplib')
const AsyncRetry = require('async-retry')
const { default: axios } = require('axios')
const CID = require('cids')
const Database = require('./helpers/Database')

if (!process.env.QUEUE_NAME || process.env.QUEUE_NAME === '') {
	throw new Error('[env] QUEUE_NAME not found')
}

const QUEUE_NAME = process.env.QUEUE_NAME
const MAX_RETRY = 5

// process event create_type
// process event transfer
const processEvent = {
	nft_create_series: async (db, next, close, msg) => {
		const session = db.client.startSession()
		session.startTransaction()

		try {
			const payload = msg.params

			// if has reference, get data
			const ref = payload.token_metadata.reference
			let metadata = payload.token_metadata
			if (ref) {
				let hash = null
				try {
					hash = new CID(ref).toString()
				} catch (err) {
					console.log(err)
					throw new Error('[nft_create_series] unknown reference')
				}

				await AsyncRetry(
					async (bail) => {
						try {
							const resp = await axios.get(`https://ipfs.fleek.co/ipfs/${hash}`)
							metadata = {
								...metadata,
								...resp.data,
							}
						} catch (err) {
							console.log(err)
							throw new Error('Try again...')
						}
					},
					{
						retries: 1000,
						maxTimeout: 5000,
					}
				)
			}

			// insert new token types
			await db.root.collection('token_series').insertOne(
				{
					token_series_id: payload.token_series_id,
					creator_id: payload.creator_id,
					price: payload.price,
					royalty: payload.royalty,
					metadata: metadata,
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activites').insertOne(
				{
					type: 'nft_create_series',
					from: null,
					to: null,
					token_id: null,
					token_series_id: payload.token_series_id,
					amount: null,
					issued_at: metadata.issued_at,
					msg: msg,
				},
				{
					session,
				}
			)

			await session.commitTransaction()
			next()
		} catch (err) {
			console.log(`[nft_create_series] error: ${err.message}`)
			await session.abortTransaction()
			close()
		} finally {
			session.endSession()
		}
	},
	nft_transfer: async (db, next, close, msg) => {
		const session = db.client.startSession()
		session.startTransaction()

		try {
			const payload = msg.params
			const [token_series_id, edition_id] = payload.token_id.split(':')

			// if mint
			if (payload.sender_id === '') {
				const token_series = await db.root.collection('token_series').findOne(
					{
						token_series_id: token_series_id,
					},
					{
						session,
					}
				)

				const royalty = token_series.royalty
				const metadata = {
					...token_series.metadata,
					...{
						title: `${token_series.metadata.title} #${edition_id}`,
					},
				}

				// mint new token
				await db.root.collection('tokens').insertOne(
					{
						token_id: payload.token_id,
						owner_id: payload.receiver_id,
						token_series_id: token_series_id,
						edition_id: edition_id,
						metadata: metadata,
						royalty: royalty,
					},
					{
						session,
					}
				)
				// add activity
				await db.root.collection('activites').insertOne(
					{
						type: 'nft_transfer',
						from: null,
						to: payload.receiver_id,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						amount: null,
						issued_at: metadata.issued_at,
						msg: msg,
					},
					{
						session,
					}
				)
			}
			// if burn
			else if (payload.receiver_id === '') {
				// update token ownership
				const result = await db.root.collection('tokens').findOneAndUpdate(
					{
						token_id: payload.token_id,
						owner_id: payload.sender_id,
					},
					{
						$set: {
							owner_id: null,
						},
					},
					{
						session,
					}
				)
				// token not found
				if (!result.value) {
					throw new Error('token_id not found')
				}

				// add activity
				await db.root.collection('activites').insertOne(
					{
						type: 'nft_transfer',
						from: payload.sender_id,
						to: null,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						amount: null,
						issued_at: msg.datetime,
						msg: msg,
					},
					{
						session,
					}
				)
			}
			// if transfer
			else {
				// update token ownership
				const result = await db.root.collection('tokens').findOneAndUpdate(
					{
						token_id: payload.token_id,
						owner_id: payload.sender_id,
					},
					{
						$set: {
							owner_id: payload.receiver_id,
						},
					},
					{
						session,
					}
				)
				// token not found
				if (!result.value) {
					throw new Error('token_id not found')
				}

				// add activity
				await db.root.collection('activites').insertOne(
					{
						type: 'nft_transfer',
						from: payload.sender_id,
						to: payload.receiver_id,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						amount: null,
						issued_at: msg.datetime,
						msg: msg,
					},
					{
						session,
					}
				)
			}

			await session.commitTransaction()
			next()
		} catch (err) {
			console.log(`[nft_transfer] error: ${err.message}`)
			await session.abortTransaction()
			close()
		} finally {
			session.endSession()
		}
	},
}

const main = async (n) => {
	if (n > MAX_RETRY) {
		console.log('[AMQP] Worker exceed max retry')
		process.exit(0)
	}
	const conn = await amqp.connect(process.env.AMQP_URL)
	const channel = await conn.createChannel()

	const database = new Database()
	await database.init()

	channel.on('close', function () {
		console.log('[AMQP] channel closed')
		console.log(`Trying again...`)
		setTimeout(() => {
			main(n + 1)
		}, 5000)
	})

	await channel.assertQueue(QUEUE_NAME, {
		durable: true,
	})

	console.log(' [*] Waiting for logs. To exit press CTRL+C')

	channel.prefetch(1)
	channel.consume(
		QUEUE_NAME,
		(msg) => {
			const parsedMsg = JSON.parse(msg.content.toString())
			const next = () => {
				console.log(' [x] Successfully indexed %s', parsedMsg.event_type)
				channel.ack(msg)
			}
			const close = () => conn.close()
			console.log('======')
			console.log(' [x] Received %s', parsedMsg.event_type)
			if (processEvent[parsedMsg.event_type]) {
				console.log(
					`[${parsedMsg.event_type}] processing ${JSON.stringify(parsedMsg)}`
				)
				processEvent[parsedMsg.event_type](database, next, close, parsedMsg)
			}
		},
		{ noAck: false }
	)
}

module.exports = main
