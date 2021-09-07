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
	nft_create_series: async (db, session, msg) => {
		try {
			const contract_id = msg.contract_id
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
							console.log(`Try again`)
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
					contract_id: contract_id,
					token_series_id: payload.token_series_id,
					creator_id: payload.creator_id,
					price: payload.price,
					lowest_price: db.toDecimal128(payload.price),
					royalty: payload.royalty,
					metadata: metadata,
					in_circulation: 0,
					updated_at: new Date(msg.datetime).getTime(),
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: contract_id,
					type: 'nft_create_series',
					from: null,
					to: null,
					token_id: null,
					token_series_id: payload.token_series_id,
					price: null,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[nft_create_series] error: ${err.message}`)
			throw err
		}
	},
	nft_transfer: async (db, session, msg) => {
		const contract_id = msg.contract_id

		try {
			const payload = msg.params
			const [token_series_id, edition_id] = payload.token_id.split(':')

			// if mint
			if (payload.sender_id === '') {
				// get token metadata
				const metadata = await AsyncRetry(
					async (bail) => {
						try {
							const token = await getToken(contract_id, payload.token_id)
							let _metadata = token.metadata
							// get metadata
							if (token.metadata && token.metadata.reference) {
								const resp = await axios.get(
									`https://ipfs.fleek.co/ipfs/${token.metadata.reference}`
								)
								_metadata = {
									...token.metadata,
									...resp.data,
								}
							}
							return _metadata
						} catch (err) {
							console.log(`Try again`)
							console.log(err)
							throw new Error('Try again...')
						}
					},
					{
						retries: 1000,
						maxTimeout: 5000,
					}
				)

				const royalty = await getRoyalty(contract_id, payload.token_id)
				// to do get royalty
				// const royalty = null
				// mint new token
				await db.root.collection('tokens').insertOne(
					{
						contract_id: contract_id,
						token_id: payload.token_id,
						owner_id: payload.receiver_id,
						token_series_id: token_series_id,
						edition_id: edition_id,
						metadata: metadata,
						royalty: royalty,
						price: null,
					},
					{
						session,
					}
				)
				const tokenSeriesExist = await db.root
					.collection('token_series')
					.findOne({
						contract_id: contract_id,
						token_series_id: token_series_id,
					})

				if (tokenSeriesExist) {
					let updateParams = {
						$inc: {
							in_circulation: 1,
							total_mint: 1,
						},
					}
					if (parseInt(metadata.copies) === parseInt(edition_id)) {
						updateParams.$set = {
							is_non_mintable: true,
						}
					}
					if (payload.price) {
						if (updateParams.$set) {
							updateParams.$set.updated_at = new Date(msg.datetime).getTime()
						} else {
							updateParams.$set = {
								updated_at: new Date(msg.datetime).getTime(),
							}
						}
					}
					// update series circulation
					await db.root.collection('token_series').findOneAndUpdate(
						{
							contract_id: contract_id,
							token_series_id: token_series_id,
						},
						updateParams,
						{
							session,
						}
					)
				} else {
					// add new series circulation
					await db.root.collection('token_series').insertOne(
						{
							contract_id: contract_id,
							token_series_id: token_series_id,
							creator_id: null,
							price: null,
							is_non_mintable: true,
							total_mint: 1,
							royalty: royalty,
							metadata: metadata,
						},
						{
							session,
						}
					)
				}

				// add activity
				await db.root.collection('activities').insertOne(
					{
						contract_id: contract_id,
						type: 'nft_transfer',
						from: payload.price
							? tokenSeriesExist.metadata.creator_id ||
							  tokenSeriesExist.contract_id
							: null,
						to: payload.receiver_id,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						price: payload.price ? db.toDecimal128(payload.price) : null,
						issued_at: new Date(msg.datetime).getTime(),
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
						contract_id: contract_id,
						token_id: payload.token_id,
						owner_id: payload.sender_id,
					},
					{
						$set: {
							owner_id: null,
							price: null,
							approval_id: null,
							ft_token_id: null,
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

				// update series circulation
				await db.root.collection('token_series').findOneAndUpdate(
					{
						contract_id: contract_id,
						token_series_id: token_series_id,
					},
					{
						$inc: {
							in_circulation: -1,
						},
					},
					{
						session,
					}
				)

				// add activity
				await db.root.collection('activities').insertOne(
					{
						contract_id: contract_id,
						type: 'nft_transfer',
						from: payload.sender_id,
						to: null,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						price: null,
						issued_at: new Date(msg.datetime).getTime(),
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
						contract_id: contract_id,
						token_id: payload.token_id,
						owner_id: payload.sender_id,
					},
					{
						$set: {
							owner_id: payload.receiver_id,
							price: null,
							approval_id: null,
							ft_token_id: null,
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
				await db.root.collection('activities').insertOne(
					{
						contract_id: contract_id,
						type: 'nft_transfer',
						from: payload.sender_id,
						to: payload.receiver_id,
						token_id: payload.token_id,
						token_series_id: token_series_id,
						price: null,
						issued_at: new Date(msg.datetime).getTime(),
						msg: msg,
					},
					{
						session,
					}
				)
			}
		} catch (err) {
			console.log(`[nft_transfer] error: ${err.message}`)
			throw err
		}
	},
	nft_set_series_price: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'comic.test.near',
		// 	block_height: 100745,
		// 	datetime: '2021-08-30T16:45:00.522770328+00:00',
		// 	event_type: 'nft_set_series_price',
		// 	params: { token_series_id: '2', price: '2000000000000000000000000' },
		// }

		try {
			const contract_id = msg.contract_id
			const { token_series_id, price } = msg.params

			const parsedPrice = price ? db.toDecimal128(price) : null

			// get new lowest price
			const query = {
				contract_id: contract_id,
				token_series_id: token_series_id,
				price: {
					$ne: null,
					$lt: parsedPrice,
				},
			}
			const tokensRaw = await db.root
				.collection('tokens')
				.find(query)
				.sort({ price: 1 })
				.limit(1)

			const tokens = await tokensRaw.toArray()

			const newLowestPrice =
				tokens.length > 0 ? tokens[0].price.toString() : null

			const updateParams = {
				$set: {
					price: parsedPrice,
					updated_at: new Date(msg.datetime).getTime(),
				},
			}

			// if it is lower than current price
			if (!newLowestPrice) {
				updateParams.$set.lowest_price = parsedPrice
			}

			const result = await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: contract_id,
					token_series_id: token_series_id,
				},
				updateParams,
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: contract_id,
					type: 'nft_set_series_price',
					from: null,
					to: null,
					token_id: null,
					token_series_id: token_series_id,
					price: parsedPrice,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error('[nft_set_series_price] token_series_id not found')
			}
		} catch (err) {
			console.log(`[nft_set_series_price] error: ${err.message}`)
			throw err
		}
	},
	nft_set_series_non_mintable: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'comic.test.near',
		// 	block_height: 101244,
		// 	datetime: '2021-08-30T16:50:03.923371541+00:00',
		// 	event_type: 'nft_set_series_non_mintable',
		// 	params: { token_series_id: '1' },
		// }

		try {
			const contract_id = msg.contract_id
			const { token_series_id } = msg.params

			const result = await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						is_non_mintable: true,
						price: null,
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error(
					'[nft_set_series_non_mintable] token_series_id not found'
				)
			}

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: contract_id,
					type: 'nft_set_series_non_mintable',
					from: null,
					to: null,
					token_id: null,
					token_series_id: token_series_id,
					price: null,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[nft_set_series_non_mintable] error: ${err.message}`)
			throw err
		}
	},
	nft_decrease_series_copies: async (db, session, msg) => {
		// const msg = {
		// 	type: 'nft_decrease_series_copies',
		// 	params: {
		// 		token_series_id: token_series_id,
		// 		copies: U64::from(token_series.metadata.copies.unwrap()),
		// 	},
		// }

		try {
			const contract_id = msg.contract_id
			const { token_series_id, copies, is_non_mintable } = msg.params

			const result = await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						'metadata.copies': copies,
						is_non_mintable: is_non_mintable,
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error(
					'[nft_decrease_series_copies] token_series_id not found'
				)
			}

			await db.root.collection('tokens').updateMany(
				{
					contract_id: contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						'metadata.copies': copies,
					},
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: contract_id,
					type: 'nft_decrease_series_copies',
					from: null,
					to: null,
					token_id: null,
					token_series_id: token_series_id,
					price: null,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[nft_set_series_non_mintable] error: ${err.message}`)
			throw err
		}
	},
	add_market_data: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'marketplace.test.near',
		// 	block_height: 105604,
		// 	datetime: '2021-08-30T17:34:15.289063819+00:00',
		// 	event_type: 'add_market_data',
		// 	params: {
		// 		owner_id: 'alice.test.near',
		// 		approval_id: 1,
		// 		nft_contract_id: 'comic.test.near',
		// 		token_id: '1:2',
		// 		ft_token_id: 'near',
		// 		price: '3000000000000000000000000',
		// 	},
		// }

		// if paras marketplace, then change price, if not then skip

		try {
			const {
				nft_contract_id,
				token_id,
				approval_id,
				owner_id,
				ft_token_id,
				price,
			} = msg.params
			const [token_series_id, edition_id] = token_id.split(':')

			const result = await db.root.collection('tokens').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_id: token_id,
					owner_id: owner_id,
				},
				{
					$set: {
						ft_token_id: ft_token_id,
						price: db.toDecimal128(price),
						approval_id: approval_id,
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error('[add_market_data] token_id not found')
			}

			// update the lowest price
			await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_series_id: token_series_id,
					$or: [
						{
							lowest_price: {
								$gt: db.toDecimal128(price),
							},
						},
						{
							lowest_price: {
								$eq: null,
							},
						},
					],
				},
				{
					$set: {
						lowest_price: db.toDecimal128(price),
						updated_at: new Date(msg.datetime).getTime(),
					},
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: nft_contract_id,
					type: 'add_market_data',
					from: null,
					to: null,
					token_id: token_id,
					token_series_id: token_series_id,
					price: db.toDecimal128(price),
					ft_token_id: ft_token_id,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[add_market_data] error: ${err.message}`)
			throw err
		}
	},
	delete_market_data: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'marketplace.test.near',
		// 	block_height: 105785,
		// 	datetime: '2021-08-30T17:36:05.592209582+00:00',
		// 	event_type: 'delete_market_data',
		// 	params: {
		// 		owner_id: 'alice.test.near',
		// 		nft_contract_id: 'comic.test.near',
		// 		token_id: '1:2',
		// 	},
		// }

		// if paras marketplace, then change price, if not then skip

		try {
			const { nft_contract_id, token_id, owner_id } = msg.params
			const [token_series_id, edition_id] = token_id.split(':')

			const result = await db.root.collection('tokens').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_id: token_id,
					owner_id: owner_id,
				},
				{
					$set: {
						ft_token_id: null,
						price: null,
						approval_id: null,
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error('[delete_market_data] token_id not found')
			}

			// get new lowest price
			const query = {
				contract_id: nft_contract_id,
				token_series_id: token_series_id,
				token_id: {
					$ne: token_id,
				},
				price: {
					$ne: null,
				},
			}
			const tokensRaw = await db.root
				.collection('tokens')
				.find(query)
				.sort({ price: 1 })
				.limit(1)

			const tokens = await tokensRaw.toArray()

			const newLowestPrice =
				tokens.length > 0 ? tokens[0].price.toString() : null

			// update the lowest price
			await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						lowest_price: newLowestPrice
							? db.toDecimal128(newLowestPrice)
							: null,
					},
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: nft_contract_id,
					type: 'delete_market_data',
					from: null,
					to: null,
					token_id: token_id,
					token_series_id: token_series_id,
					price: null,
					ft_token_id: null,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[delete_market_data] error: ${err.message}`)
			throw err
		}
	},
	update_market_data: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'marketplace.test.near',
		// 	block_height: 105604,
		// 	datetime: '2021-08-30T17:34:15.289063819+00:00',
		// 	event_type: 'add_market_data',
		// 	params: {
		// 		owner_id: 'alice.test.near',
		// 		approval_id: 1,
		// 		nft_contract_id: 'comic.test.near',
		// 		token_id: '1:2',
		// 		ft_token_id: 'near',
		// 		price: '3000000000000000000000000',
		// 	},
		// }

		// if paras marketplace, then change price, if not then skip

		try {
			const { nft_contract_id, token_id, owner_id, ft_token_id, price } =
				msg.params
			const [token_series_id, edition_id] = token_id.split(':')

			const result = await db.root.collection('tokens').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_id: token_id,
					owner_id: owner_id,
				},
				{
					$set: {
						ft_token_id: ft_token_id,
						price: db.toDecimal128(price),
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error('[update_market_data] token_id not found')
			}

			// get new lowest price
			const query = {
				contract_id: nft_contract_id,
				token_series_id: token_series_id,
				token_id: {
					$ne: token_id,
				},
				price: {
					$ne: null,
					$lt: db.toDecimal128(price),
				},
			}
			const tokensRaw = await db.root
				.collection('tokens')
				.find(query)
				.sort({ price: 1 })
				.limit(1)

			const tokens = await tokensRaw.toArray()

			const newLowestPrice =
				tokens.length > 0 ? tokens[0].price.toString() : price

			// throw new Error('asd')
			// update the lowest price
			await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						lowest_price: newLowestPrice
							? db.toDecimal128(newLowestPrice)
							: null,
						updated_at: new Date().getTime(),
					},
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: nft_contract_id,
					type: 'update_market_data',
					from: null,
					to: null,
					token_id: token_id,
					token_series_id: token_series_id,
					price: db.toDecimal128(price),
					ft_token_id: ft_token_id,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[update_market_data] error: ${err.message}`)
			throw err
		}
	},
	resolve_purchase: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'marketplace.test.near',
		// 	block_height: 107075,
		// 	datetime: '2021-08-30T17:49:10.983520343+00:00',
		// 	event_type: 'resolve_purchase',
		// 	params: {
		// 		owner_id: 'alice.test.near',
		// 		nft_contract_id: 'comic.test.near',
		// 		token_id: '1:5',
		// 		ft_token_id: 'near',
		// 		price: '3000000000000000000000000',
		// 		buyer_id: 'bob.test.near',
		// 	},
		// }

		// if paras marketplace, then change price, if not then skip

		try {
			const {
				nft_contract_id,
				token_id,
				owner_id,
				ft_token_id,
				price,
				buyer_id,
			} = msg.params
			const [token_series_id, edition_id] = token_id.split(':')

			// update token_series
			await db.root.collection('token_series').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_series_id: token_series_id,
				},
				{
					$set: {
						updated_at: new Date(msg.datetime).getTime(),
					},
				},
				{
					session,
				}
			)

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: nft_contract_id,
					type: 'resolve_purchase',
					from: owner_id,
					to: buyer_id,
					token_id: token_id,
					token_series_id: token_series_id,
					price: db.toDecimal128(price),
					ft_token_id: ft_token_id,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)

			// updated_at: new Date().getTime()
		} catch (err) {
			console.log(`[resolve_purchase] error: ${err.message}`)
			throw err
		}
	},
	resolve_purchase_fail: async (db, session, msg) => {
		// const msg = {
		// 	contract_id: 'marketplace.test.near',
		// 	block_height: 107075,
		// 	datetime: '2021-08-30T17:49:10.983520343+00:00',
		// 	event_type: 'resolve_purchase_fail',
		// 	params: {
		// 		owner_id: 'alice.test.near',
		// 		nft_contract_id: 'comic.test.near',
		// 		token_id: '1:5',
		// 		ft_token_id: 'near',
		// 		price: '3000000000000000000000000',
		// 		buyer_id: 'bob.test.near',
		// 	},
		// }

		// if paras marketplace, then change price, if not then skip

		try {
			const { nft_contract_id, token_id, owner_id, buyer_id } = msg.params
			const [token_series_id, edition_id] = token_id.split(':')

			const result = await db.root.collection('tokens').findOneAndUpdate(
				{
					contract_id: nft_contract_id,
					token_id: token_id,
					owner_id: owner_id,
				},
				{
					$set: {
						ft_token_id: null,
						price: null,
						approval_id: null,
					},
				},
				{
					session,
				}
			)

			// token not found
			if (!result.value) {
				throw new Error('[resolve_purchase_fail] token_id not found')
			}

			// add activity
			await db.root.collection('activities').insertOne(
				{
					contract_id: nft_contract_id,
					type: 'resolve_purchase_fail',
					from: owner_id,
					to: buyer_id,
					token_id: token_id,
					token_series_id: token_series_id,
					price: null,
					ft_token_id: null,
					issued_at: new Date(msg.datetime).getTime(),
					msg: msg,
				},
				{
					session,
				}
			)
		} catch (err) {
			console.log(`[resolve_purchase] error: ${err.message}`)
			throw err
		}
	},
}

const processQueue = async (db, next, close, msg) => {
	if (process.env.FIRST_BLOCK_HEIGHT) {
		const firstBlockHeight = parseInt(process.env.FIRST_BLOCK_HEIGHT)
		const currBlockHeight = parseInt(db.block_height)
		if (firstBlockHeight > currBlockHeight) {
			next()
		}
	}

	const session = db.client.startSession()
	session.startTransaction()

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
				await processEvent[formatEvent.event_type](db, session, formatEvent)
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
			processQueue(database, next, close, parsedMsg)
		},
		{ noAck: false }
	)
}

module.exports = main
