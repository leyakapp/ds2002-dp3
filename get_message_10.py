import boto3
from botocore.exceptions import ClientError

# Set up your SQS queue URL and boto3 client
url = "https://sqs.us-east-1.amazonaws.com/440848399208/mth8yq"
sqs = boto3.client('sqs')

# Dictionary to store messages with their order as key
messages = {}

def delete_messages(handles):
    """Delete messages from the SQS queue using the receipt handles."""
    for handle in handles:
        try:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=handle)
            print(f"Message with handle {handle} deleted")
        except ClientError as e:
            print(f"Error deleting message: {e.response['Error']['Message']}")

def get_messages():
    """Retrieve messages from the SQS queue and store them in a dictionary."""
    receipt_handles = []
    attempts = 0
    while len(receipt_handles) < 10 and attempts < 5:
        response = sqs.receive_message(
            QueueUrl=url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All']
        )

        if 'Messages' in response:
            for message in response['Messages']:
                order = message['MessageAttributes']['order']['StringValue']
                word = message['MessageAttributes']['word']['StringValue']
                handle = message['ReceiptHandle']

                messages[int(order)] = word  # Ensure the order is an integer for sorting
                receipt_handles.append(handle)
                print(f"Retrieved: {word} (Order: {order})")
        else:
            print("No messages retrieved, retrying...")
        attempts += 1

    return receipt_handles

def reassemble_message():
    """Assemble the message content from stored message parts."""
    sorted_words = [messages[key] for key in sorted(messages.keys())]
    return ' '.join(sorted_words)

# Main execution
if __name__ == "__main__":
    try:
        # Get messages from the queue
        handles = get_messages()
        
        # Reassemble the message from parts
        phrase = reassemble_message()
        print(f"Reassembled phrase: {phrase}")
        
        # Uncomment the following line when ready to delete messages permanently
        # delete_messages(handles)

    except Exception as e:
        print(f"An error occurred: {e}")
