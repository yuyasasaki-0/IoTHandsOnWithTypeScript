import AWS from 'aws-sdk'
import { PutRecordsOutput, PutRecordsResultEntry } from 'aws-sdk/clients/kinesis'

const REGION = 'ap-northeast-1'
const TABLE_NAME = process.env.table === undefined ? '' : process.env.table
const DDB_PRIMARY_KEY = 'deviceid'
const DDB_SORT_KEY = 'timestamp'

AWS.config.update({ region: REGION })

const dynomodb = new AWS.DynamoDB({ apiVersion: '2012-08-10' })

interface dynamoDBPutDataInterface {
  TableName: string
  Item: {
    [DDB_PRIMARY_KEY]: { S: string }
    [DDB_SORT_KEY]: { S: string }
    HUMIDITY: { N: string }
    TEMPERATURE: { N: string }
  }
}

interface payloadType {
  DEVICE_NAME: string
  TIMESTAMP: string
  HUMIDITY: string
  TEMPERATURE: string
}

const convertData = (strData: string): dynamoDBPutDataInterface => {
  const payloadObject: payloadType = JSON.parse(strData) as payloadType
  const putData: dynamoDBPutDataInterface = {
    TableName: TABLE_NAME,
    Item: {
      [DDB_PRIMARY_KEY]: {
        S: payloadObject.DEVICE_NAME
      },
      [DDB_SORT_KEY]: {
        S: payloadObject.TIMESTAMP
      },
      HUMIDITY: { N: payloadObject.HUMIDITY },
      TEMPERATURE: { N: payloadObject.TEMPERATURE }
    }
  }
  console.log(`putData: ${JSON.stringify(putData)}`)
  return putData
}

const itemInfosToWrite = (dataArray: string[]): dynamoDBPutDataInterface[] => {
  return dataArray.map((data) => (
    convertData(data)
  ))
}

const dynamoBulkPut = (dataArray: string[]): void => {
  try {
    const items = itemInfosToWrite(dataArray)
    items.forEach(item => {
      dynomodb.putItem(item, (err, data) => {
        if (err !== undefined && err !== null) {
          console.log('Error', err)
        } else {
          console.log('Success', data)
        }
      })
    })
  } catch (e) {
    console.log(`Error while putting decoded data to DynamoDB, ${JSON.stringify(e)}`)
    throw e
  }
}

const handler = (event: PutRecordsOutput): string => {
  const decodedKinesisDataArray = event.Records.map((record: PutRecordsResultEntry) => {
    // Kinesis data is base64 encoded so decode here
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    const payload = Buffer.from((record as any).kinesis.data, 'base64').toString('ascii')
    console.log('Decoded payload:', payload)
    return payload
  })
  if (decodedKinesisDataArray.length > 0) {
    dynamoBulkPut(decodedKinesisDataArray)
  } else {
    console.log('there is no valid data in Kinesis stream, all data passed')
  }
  return JSON.stringify(event, null, 2)
}

export { handler }
