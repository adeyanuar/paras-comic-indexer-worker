require('dotenv').config()
const amqp = require('amqplib')
const AsyncRetry = require('async-retry')
const { default: axios } = require('axios')
const CID = require('cids')
const Database = require('./helpers/Database')
const nearApi = require('near-api-js')
const nearConfig = require('./near.config.json')

if (!process.env.QUEUE_NAME || process.env.QUEUE_NAME === '') {
	throw new Error('[env] QUEUE_NAME not found')
}

if (!nearConfig) {
	console.log(`near.config.json not found`)
	process.exit(1)
}

const QUEUE_NAME = process.env.QUEUE_NAME
const MAX_RETRY = 5

// process event create_type
// process event transfer
const processEvent = {
	create_type: async (db, session, msg) => {
		try {
			const payload = msg.params
			const [comic_id, chapter_id] = payload.token_type.split('-')
			// if has reference, get data
			const ref = payload.token_metadata.reference
			let metadata = payload.token_metadata
			if (ref) {
				try {
					const cid = new CID(ref)
					const resp = await axios.get(
						`https://ipfs.fleek.co/ipfs/${cid.toString()}`
					)
					metadata = {
						...metadata,
						...resp.data,
					}
				} catch (err) {
					console.log(err)
					throw new Error('[create_type] unknown reference')
				}
			}

			// insert new token types
			await db.root.collection('token_types').insertOne(
				{
					token_type: payload.token_type,
					comic_id: comic_id,
					chapter_id: parseInt(chapter_id),
					metadata: metadata,
					price: payload.price,
				},
				{
					session,
				}
			)

			// insert new chapter
			if (metadata.comic_id && metadata.chapter_id) {
				await db.root.collection('chapters').insertOne(
					{
						token_type: payload.token_type,
						comic_id: comic_id,
						chapter_id: parseInt(chapter_id),
						metadata: metadata,
						price: payload.price,
					},
					{
						session,
					}
				)
			}

			// add activity
			await db.root.collection('activities').insertOne(
				{
					type: 'create_type',
					from: null,
					to: null,
					token_id: null,
					token_type: payload.token_type,
					amount: null,
					issued_at: metadata.issued_at,
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[create_type] error: ${err.message}`)
			throw err
		}
	},
	mint: async (db, session, msg) => {
		try {
			const payload = msg.params
			// if has reference, get data
			const ref = payload.metadata.reference
			let metadata = payload.metadata
			if (ref) {
				try {
					const cid = new CID(ref)
					const resp = await axios.get(
						`https://ipfs.fleek.co/ipfs/${cid.toString()}`
					)
					metadata = {
						...metadata,
						...resp.data,
					}
				} catch (err) {
					console.log(err)
					throw new Error('[mint] unknown token reference')
				}
			}

			const [token_type, edition_id] = payload.token_id.split(':')
			const [comic_id, chapter_id] = token_type.split('-')

			// mint new token
			await db.root.collection('tokens').insertOne(
				{
					token_id: payload.token_id,
					token_type: token_type,
					comic_id: comic_id,
					chapter_id: parseInt(chapter_id),
					edition_id: parseInt(edition_id),
					metadata: metadata,
					owner_id: payload.owner_id,
				},
				{
					session,
				}
			)
			// add user access to chapter
			await db.root.collection('access').findOneAndUpdate(
				{
					account_id: payload.owner_id,
					comic_id: comic_id,
					chapter_id: parseInt(chapter_id),
				},
				{
					$push: {
						access_tokens: payload.token_id,
					},
				},
				{
					upsert: true,
					session,
				}
			)
			// add activity
			await db.root.collection('activities').insertOne(
				{
					type: 'mint',
					from: null,
					to: payload.owner_id,
					token_id: payload.token_id,
					token_type: token_type,
					amount: null,
					issued_at: metadata.issued_at,
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[mint] error: ${err.message}`)
			throw err
		}
	},
	transfer: async (db, session, msg) => {
		try {
			const payload = msg.params
			const [token_type, edition_id] = payload.token_id.split(':')
			const [comic_id, chapter_id] = token_type.split('-')

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
			// remove chapter access from sender
			await db.root.collection('access').findOneAndUpdate(
				{
					account_id: payload.sender_id,
					comic_id: comic_id,
					chapter_id: parseInt(chapter_id),
				},
				{
					$pull: {
						access_tokens: payload.token_id,
					},
				},
				{
					session,
				}
			)
			// add chapter access to receiver
			await db.root.collection('access').findOneAndUpdate(
				{
					account_id: payload.receiver_id,
					comic_id: comic_id,
					chapter_id: parseInt(chapter_id),
				},
				{
					$push: {
						access_tokens: payload.token_id,
					},
				},
				{
					upsert: true,
					session,
				}
			)
			// add activity
			await db.root.collection('activities').insertOne(
				{
					type: 'transfer',
					from: payload.sender_id,
					to: payload.receiver_id,
					token_id: payload.token_id,
					token_type: token_type,
					amount: null,
					issued_at: msg.datetime,
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[transfer] error: ${err.message}`)
			throw err
		}
	},
}

const processQueue = async (db, next, close, msg) => {
	try {
		for await (const event of msg.events) {
			const formatEvent = {
				contract_id: event.contract_id,
				block_height: msg.block_height,
				datetime: msg.datetime,
				event_type: event.event_type,
				params: event.params,
			}

			if (processEvent[formatEvent.event_type]) {
				console.log(
					`[${formatEvent.event_type}] processing ${JSON.stringify(
						formatEvent
					)}`
				)
				processEvent[formatEvent.event_type](db, session, formatEvent)
			}
		}

		await session.commitTransaction()
		next()
	} catch (err) {
		console.log(
			`${new Date().toISOString()} [queue::${msg.block_height}] error: ${
				err.message
			}`
		)
		await session.abortTransaction()
		close()
	} finally {
		session.endSession()
	}
}

let getToken = () => {}
let getRoyalty = () => {}

const main = async (n) => {
	if (n > MAX_RETRY) {
		console.log('[AMQP] Worker exceed max retry')
		process.exit(0)
	}
	const conn = await amqp.connect(process.env.AMQP_URL)
	const channel = await conn.createChannel()

	const keyStore = new nearApi.keyStores.UnencryptedFileSystemKeyStore(
		`${process.env.HOME}/.near-credentials`
	)
	const near = await nearApi.connect({
		networkId: nearConfig.networkId,
		deps: { keyStore },
		masterAccount: nearConfig.contractName,
		nodeUrl: nearConfig.nodeUrl,
	})

	getToken = async (contractId, token_id) => {
		try {
			const account = await near.account(nearConfig.contractName)
			const token = await account.viewFunction(contractId, 'nft_token', {
				token_id: token_id,
			})
			return token
		} catch (err) {
			throw err
		}
	}

	getRoyalty = async (contractId, token_id) => {
		try {
			const account = await near.account(nearConfig.contractName)
			const result = await account.viewFunction(contractId, 'nft_payout', {
				token_id: token_id,
				balance: `10000`,
				max_len_payout: 10,
			})
			return result
		} catch (err) {
			throw err
		}
	}

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
				console.log(' [x] Successfully indexed %s', parsedMsg.block_height)
				channel.ack(msg)
			}
			const close = () => conn.close()
			console.log('======')
			console.log(' [x] Received %s', parsedMsg.block_height)
			processQueue(db, next, close, parsedMsg)
		},
		{ noAck: false }
	)
}

module.exports = main
