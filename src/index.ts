const handler = async (event: Promise<Record<string, string>> = new Promise(() => ({ test: 'data' }))): Promise<string> => {
  console.log('Hello World!')
  return JSON.stringify(event, null, 2)
}

export { handler }
