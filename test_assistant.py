import sys
from openai import OpenAI
from config.settings import settings

def start_chat_session():
    """
    Creates a terminal-based chat interface to test the SCAPILE Assistant.
    Uses the client's API Key and Assistant ID directly from the .env file.
    """
    print("\n" + "="*50)
    print("   SCAPILE - LIVE TERMINAL TESTING ENVIRONMENT (DEBUG MODE)")
    print("="*50)

    # Validate that IDs exist in .env
    if not settings.OPENAI_API_KEY or not settings.OPENAI_ASSISTANT_ID:
        print("\n❌ ERROR: API Key or Assistant ID is missing in your .env file!")
        print("Make sure you added OPENAI_ASSISTANT_ID=asst_... to the .env file.")
        sys.exit(1)

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    assistant_id = settings.OPENAI_ASSISTANT_ID

    print(f"✅ Connected to Assistant ID: {assistant_id}")
    print("💡 Type 'exit' or 'quit' to end the session.\n")

    try:
        # Create a fresh thread (chat history) for this session
        thread = client.beta.threads.create()
        print(f"🟢 New Chat Session Started (Thread ID: {thread.id})\n")

        while True:
            # Get user input
            user_input = input("🗣️ You: ")
            
            if user_input.lower() in ['exit', 'quit']:
                print("\nEnding session. Goodbye!")
                break
            if not user_input.strip():
                continue

            # 1. Add user message to the thread
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=user_input
            )

            print("⏳ SCAPILE is reading files and thinking...", end="\r")

            # 2. Run the Assistant and wait for it to finish
            run = client.beta.threads.runs.create_and_poll(
                thread_id=thread.id,
                assistant_id=assistant_id
            )

            # Clear the thinking line
            print(" " * 50, end="\r") 

            # 3. Analyze the response status
            if run.status == 'completed':
                messages = client.beta.threads.messages.list(thread_id=thread.id)
                # The latest message is always at index 0
                latest_message = messages.data[0].content[0].text.value
                
                print(f"🤖 SCAPILE:\n{latest_message}\n")
                print("-" * 50)
                
            elif run.status == 'failed':
                print(f"\n❌ Error: Assistant run ended with status: {run.status}")
                # THIS IS THE NEW DEBUGGING PART
                if run.last_error:
                    print(f"🛑 Error Code: {run.last_error.code}")
                    print(f"🛑 Error Message: {run.last_error.message}\n")
                else:
                    print("🛑 No specific error message provided by OpenAI.\n")
                print("-" * 50)
                
            else:
                print(f"\n⚠️ Unexpected Status: {run.status}\n")
                print("-" * 50)

    except KeyboardInterrupt:
        print("\n\nSession interrupted. Goodbye!")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    start_chat_session()