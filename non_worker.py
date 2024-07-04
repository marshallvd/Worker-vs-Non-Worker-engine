import asyncio
import time
import traceback
from sqlalchemy import create_engine, Table, MetaData, select, func, text
from sqlalchemy.orm import sessionmaker

# Database configuration
engine = create_engine('mysql+mysqlconnector://root:@127.0.0.1/db_tele_complex', echo=False)
Session = sessionmaker(bind=engine)
metadata = MetaData()

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

async def simulate_interaction(user_id, user_input):
    start_time = time.time()

    simulate_user_input(user_id, user_input)

    if user_input in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']:
        total_limit = int(user_input) * 200
        
        stories = fetch_stories_basic(total_limit, 0)

        if not stories:
            message = "Tidak ada cerita yang ditemukan."
            print(message)
            simulate_bot_response(user_id, message)
            elapsed_time = time.time() - start_time
            print(f"\nWaktu respons ({user_input} data): {elapsed_time:.2f} detik\n")
            return elapsed_time

        story_ids = [story['id'] for story in stories]
        
        avg_ratings = fetch_avg_ratings(story_ids)
        comment_counts = fetch_comment_counts(story_ids)

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

        # Print and send response to outbox as one message
        print("=" * 50)
        print("Hasil Query:")
        print("=" * 50)
        print(combined_response)
        print("=" * 50)

        simulate_bot_response(user_id, combined_response)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nWaktu respons ({total_limit} data): {elapsed_time:.2f} detik\n")
        return elapsed_time

    else:
        message = "Maaf, saya tidak mengerti. Silakan pilih opsi dari menu."
        print(message)
        simulate_bot_response(user_id, message)
        elapsed_time = time.time() - start_time
        print(f"\nWaktu respons: {elapsed_time:.2f} detik\n")
        return elapsed_time


def print_menu():
    print("Menu Non worker:")
    for i in range(1, 11):
        print(f"{i}. Fetch {i * 200} stories")
    print("q. Quit\n\n")

async def main():
    try:
        while True:
            print_menu()
            user_id = "marshall"
            user_input = input("Masukkan pilihan menu: ")

            if user_input.lower() == 'q':
                break

            elapsed_time = await simulate_interaction(user_id, user_input)
            # print(f"\nWaktu respons ({user_input} data): {elapsed_time:.2f} detik\n")

    except KeyboardInterrupt:
        print("\nProgram dihentikan oleh pengguna.")

if __name__ == '__main__':
    asyncio.run(main())
