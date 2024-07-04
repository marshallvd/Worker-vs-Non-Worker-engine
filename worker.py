import asyncio
import time
import traceback
from multiprocessing import Process, Queue
from sqlalchemy import create_engine, Table, MetaData, select, func, text
from sqlalchemy.orm import sessionmaker
import math

# Database configuration
engine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/db_tele_complex', echo=False)
Session = sessionmaker(bind=engine)
metadata = MetaData()

# Number of workers
NUM_WORKERS = 8

# Worker function
def worker_function(task_queue, result_queue):
    while True:
        task = task_queue.get()
        if task is None:
            break
        function, args = task
        try:
            result = function(*args)
            result_queue.put(result)
        except Exception as e:
            print(f"Error in worker: {e}")
            print(f"Function: {function.__name__}, Args: {args}")
            traceback.print_exc()
            result_queue.put(None)

def simulate_user_input(user_id, message):
    session = Session()
    try:
        inbox_table = Table('tb_inbox', metadata, autoload_with=engine)
        session.execute(inbox_table.insert().values(user_id=user_id, message=message))
        session.commit()
    except Exception as e:
        print(f"Error in simulate_user_input: {e}")
        traceback.print_exc()
    finally:
        session.close()

def simulate_bot_response(user_id, message):
    session = Session()
    try:
        outbox_table = Table('tb_outbox', metadata, autoload_with=engine)
        session.execute(outbox_table.insert().values(user_id=user_id, message=message))
        session.commit()
    except Exception as e:
        print(f"Error in simulate_bot_response: {e}")
        traceback.print_exc()
    finally:
        session.close()

def fetch_stories_basic(limit, offset):
    session = Session()
    try:
        query = text("""
            SELECT 
                s.id, s.title, a.name as author_name, c.name as category_name,
                CHAR_LENGTH(s.content) as content_length,
                DATEDIFF(CURRENT_TIMESTAMP, s.created_at) as days_since_creation
            FROM stories s
            JOIN authors a ON s.author_id = a.id
            JOIN story_categories c ON s.category_id = c.id
            ORDER BY s.id
            LIMIT :limit OFFSET :offset
        """)
        
        result = session.execute(query, {'limit': limit, 'offset': offset})
        
        stories = [dict(row._mapping) for row in result]
        return stories
    except Exception as e:
        print(f"Error in fetch_stories_basic: {e}")
        traceback.print_exc()
        return None
    finally:
        session.close()

def fetch_avg_ratings(story_ids):
    session = Session()
    try:
        ratings = Table('ratings', metadata, autoload_with=engine)
        query = select(
            ratings.c.story_id,
            func.avg(ratings.c.rating).label('avg_rating')
        ).where(ratings.c.story_id.in_(story_ids)).group_by(ratings.c.story_id)
        result = session.execute(query)
        return {row.story_id: row.avg_rating for row in result}
    except Exception as e:
        print(f"Error in fetch_avg_ratings: {e}")
        traceback.print_exc()
        return {}
    finally:
        session.close()

def fetch_comment_counts(story_ids):
    session = Session()
    try:
        comments = Table('comments', metadata, autoload_with=engine)
        query = select(
            comments.c.story_id,
            func.count(comments.c.id).label('comment_count')
        ).where(comments.c.story_id.in_(story_ids)).group_by(comments.c.story_id)
        result = session.execute(query)
        return {row.story_id: row.comment_count for row in result}
    except Exception as e:
        print(f"Error in fetch_comment_counts: {e}")
        traceback.print_exc()
        return {}
    finally:
        session.close()

async def simulate_interaction(user_id, user_input, task_queue, result_queue):
    start_time = time.time()

    simulate_user_input(user_id, user_input)

    if user_input in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']:
        total_limit = int(user_input) * 200
        chunk_size = math.ceil(total_limit / NUM_WORKERS)  # Divide total stories by number of workers
        
        stories = []
        for offset in range(0, total_limit, chunk_size):
            limit = min(chunk_size, total_limit - offset)
            task_queue.put((fetch_stories_basic, (limit, offset)))
        
        for _ in range(NUM_WORKERS):
            chunk_result = result_queue.get()
            if chunk_result is not None:
                stories.extend(chunk_result)
            else:
                print("Received None result from worker")

        if not stories:
            message = "Tidak ada cerita yang ditemukan."
            print(message)
            simulate_bot_response(user_id, message)
            return time.time() - start_time

        story_ids = [story['id'] for story in stories]
        
        task_queue.put((fetch_avg_ratings, (story_ids,)))
        task_queue.put((fetch_comment_counts, (story_ids,)))
        
        avg_ratings = result_queue.get() or {}
        comment_counts = result_queue.get() or {}

        for story in stories:
            story['average_rating'] = avg_ratings.get(story['id'], 0)
            story['total_comments'] = comment_counts.get(story['id'], 0)

        # Sort stories based on id
        stories.sort(key=lambda x: x['id'])

        # Combine all results in one string
        combined_response = "\n".join(
            f"({story['id']}, '{story['title']}', '{story['author_name']}', '{story['category_name']}', {story['average_rating']:.2f}, {story['total_comments']})"
            for story in stories
        )

        # Mark the beginning and end of output with boundary lines
        print("=" * 50)
        print("Query Results:")
        print("=" * 50)
        print(combined_response)
        print("=" * 50)

        # Print and send response to outbox as one message
        simulate_bot_response(user_id, combined_response)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nResponse time ({total_limit} data): {elapsed_time:.2f} seconds\n")

    else:
        message = "Sorry, I don't understand. Please choose an option from the menu."
        print(message)
        simulate_bot_response(user_id, message)

    return time.time() - start_time

def print_menu():
    print("Worker Menu:")
    for i in range(1, 11):
        print(f"{i}. Fetch {i * 200} stories")
    print("q. Quit\n\n")

async def main():
    task_queue = Queue()
    result_queue = Queue()

    # Start worker processes
    workers = []
    for _ in range(NUM_WORKERS):
        worker = Process(target=worker_function, args=(task_queue, result_queue))
        worker.start()
        workers.append(worker)

    try:
        while True:
            print_menu()
            user_id = "marshall"
            user_input = input("Enter menu choice: ")

            if user_input.lower() == 'q':
                break

            elapsed_time = await simulate_interaction(user_id, user_input, task_queue, result_queue)

    finally:
        # Clean up
        for _ in range(NUM_WORKERS):
            task_queue.put(None)
        for worker in workers:
            worker.join()

if __name__ == '__main__':
    asyncio.run(main())