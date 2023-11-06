from sqlalchemy import func, desc, select, and_
from sqlalchemy.orm import sessionmaker

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session, engine

Session = sessionmaker(bind=engine)
session = Session()

def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id, s.fullname
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
	    s.id AS student_id,
	    s.fullname AS student_name,
	    ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    WHERE g.subject_id = 1
    GROUP BY s.id, s.fullname
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        g.id AS group_id,
        g.name AS group_name,
        AVG(grades.grade) AS average_grade
    FROM groups g
    JOIN students s ON g.id = s.group_id
    JOIN grades ON s.id = grades.student_id
    WHERE grades.subjects_id = 3
    GROUP BY g.id, g.name
    ORDER BY average_grade DESC;
    """
    result = session.query(Group.id.label('group_id'), Group.name.label('group_name'),
                            func.avg(Grade.grade).label('average_grade')) \
        .select_from(Group).join(Student, Group.id == Student.group_id) \
        .join(Grade, Student.id == Grade.student_id) \
        .filter(Grade.subjects_id == 4).group_by(Group.id, Group.name) \
        .order_by(desc('average_grade')).all()
    return result


def select_04():
    """
    SELECT AVG(grade) AS average_grade
    FROM grades;
    """
    result = session.query(func.avg(Grade.grade).label('average_grade')).first()
    return result


def select_05():
    """
    SELECT s.name AS subject_name
    FROM subjects s
    WHERE s.teacher_id = 2;
    """
    result = session.query(Subject.name.label('subject_name')) \
        .filter(Subject.teacher_id == 2).all()
    return result


def select_06():
    """
    SELECT id, fullname
    FROM students
    WHERE group_id = 3;
    """
    result = session.query(Student.id, Student.fullname) \
        .filter(Student.group_id == 3).all()
    return result


def select_07():
    """
    SELECT
        s.fullname AS student_name,
        g.grade,
        g.grade_date
    FROM students s
    JOIN grades g ON s.id = g.student_id
    WHERE s.group_id = 3 AND g.subjects_id = 3;
    """
    result = session.query(Student.fullname.label('student_name'), Grade.grade, Grade.grade_date) \
        .join(Grade, Student.id == Grade.student_id) \
        .filter(and_(Student.group_id == 3, Grade.subjects_id == 3)).all()
    return result


def select_08():
    """
    SELECT AVG(g.grade) AS average_grade
    FROM grades g
    JOIN subjects s ON g.subjects_id = s.id
    WHERE s.teacher_id = 2;
    """
    result = session.query(func.avg(Grade.grade).label('average_grade')) \
        .join(Subject, Grade.subjects_id == Subject.id) \
        .filter(Subject.teacher_id == 2).first()
    return result


def select_09():
    """
    SELECT DISTINCT s.name AS subject_name
    FROM subjects s
    JOIN grades g ON s.id = g.subjects_id
    JOIN students stu ON g.student_id = stu.id
    WHERE stu.id = 2;
    """
    result = session.query(Subject.name.label('subject_name')) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .join(Student, Grade.student_id == Student.id) \
        .filter(Student.id == 2).distinct().all()
    return result


def select_10():
    """
    SELECT s.name AS subject_name
    FROM subjects s
    JOIN grades g ON s.id = g.subjects_id
    JOIN students stu ON g.student_id = stu.id
    JOIN teachers t ON s.teacher_id = t.id
    WHERE stu.id = 3 AND t.id = 3;
    """
    result = session.query(Subject.name.label('subject_name')) \
        .join(Grade, Subject.id == Grade.subjects_id) \
        .join(Student, Grade.student_id == Student.id) \
        .join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(and_(Student.id == 5, Teacher.id == 4)).all()
    return result


if __name__ == '__main__':
    print(select_01())
