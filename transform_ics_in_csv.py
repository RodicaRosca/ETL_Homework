import pandas as pd
from icalendar import Calendar
from datetime import datetime, timedelta, date

def parse_ics(ics_path, csv_path):
    with open(ics_path, 'r', encoding='utf-8') as f:
        cal = Calendar.from_ical(f.read())

    events = []

    for component in cal.walk():
        if component.name == "VEVENT":
            summary = str(component.get('summary', ''))
            category = str(component.get('categories', ''))
            location = str(component.get('location', ''))
            description = str(component.get('description', ''))
            organizer = component.get('organizer')
            if organizer:
                organizer_email = organizer.to_ical().decode().split(':')[-1]
                organizer_cn = organizer.params.get('CN', '')
            else:
                organizer_email = ''
                organizer_cn = ''
            
            dtstart = component.get('dtstart').dt
            dtend = component.get('dtend').dt

            attendees = component.get('attendee')
            attendee_names = []
            attendee_emails = []
            if attendees:
                if not isinstance(attendees, list):
                    attendees = [attendees]
                for a in attendees:
                    name = a.params.get('CN', '')
                    mail = str(a).split(':')[-1]
                    attendee_names.append(name)
                    attendee_emails.append(mail)
   
            if isinstance(dtstart, datetime) and isinstance(dtend, datetime):
                duration = dtend - dtstart
            elif isinstance(dtstart, datetime) and isinstance(dtend, date):
                duration = dtend - dtstart.date()
            elif isinstance(dtstart, date) and isinstance(dtend, date):
                duration = dtend - dtstart
            else:
                duration = timedelta(0)
            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)

            events.append({
                'Summary': summary,
                'Category': category,
                'Location': location,
                'Description': description,
                'Start': dtstart,
                'End': dtend,
                'Duration': f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
                'Organizer Name': organizer_cn,
                'Organizer Email': organizer_email,
                'Attendees Names': ', '.join(attendee_names),
                'Attendees Emails': ', '.join(attendee_emails)
            })

    df = pd.DataFrame(events)
    df.to_csv(csv_path, index=False)
    print(f"Exported {len(df)} events to {csv_path}")

parse_ics('abs_exam.ics', 'abs_exam.csv')
