from flask import Flask, render_template, jsonify, request
import pandas as pd
import json
from collections import Counter

app = Flask(__name__)

# Global variable to store our data
job_data = None

def load_job_data():
    """Load and process the XLSX job data"""
    global job_data
    try:
        df = pd.read_excel('job_data.xlsx')
        df['auto_score'] = pd.to_numeric(df['auto_score'], errors='coerce')
        df['manual_score'] = pd.to_numeric(df['manual_score'], errors='coerce')
        df.dropna(subset=['auto_score', 'manual_score'], inplace=True)
        df['Automatability_Analysis_Parsed'] = df['Automatability_Analysis'].apply(
            lambda x: json.loads(x) if pd.notna(x) and isinstance(x, str) and x.strip().startswith('[') else []
        )
        job_data = df
        print(f"Successfully loaded {len(df)} job records")
        return True
    except Exception as e:
        print(f"Error loading job data: {e}")
        return False

@app.route('/')
def index():
    if job_data is None and not load_job_data():
        return "Error: Could not load job data."
    return render_template('index.html')

@app.route('/api/categories')
def get_categories():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    level = request.args.get('level', '4')  # Default to level 4
    
    level_column = f'level_{level}_name'
    if level_column not in job_data.columns:
        return jsonify({'error': f'Level {level} not available'}), 400
    
    # Get categories with their job counts and sort by frequency (descending)
    category_counts = job_data[level_column].value_counts().to_dict()
    categories_with_counts = [
        {
            'name': category,
            'count': count
        }
        for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    
    return jsonify(categories_with_counts)

@app.route('/api/levels')
def get_available_levels():
    """Get all available filtering levels"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    levels = []
    for i in range(1, 5):  # Check levels 1-4
        level_column = f'level_{i}_name'
        if level_column in job_data.columns:
            levels.append({
                'level': i,
                'name': f'Level {i}',
                'count': job_data[level_column].nunique()
            })
    
    return jsonify(levels)

@app.route('/api/stats')
def get_stats():
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    # Calculate sector count
    sector_count = 0
    if 'Sector' in job_data.columns:
        sector_count = job_data['Sector'].nunique()
    
    stats = {
        'total_jobs': len(job_data),
        'unique_level_4_categories': job_data['level_4_name'].nunique(),
        'sector_count': sector_count,
        'avg_auto_score': round(job_data['auto_score'].mean(), 1),
        'avg_manual_score': round(job_data['manual_score'].mean(), 1)
    }
    return jsonify(stats)

@app.route('/api/jobs')
def get_jobs():
    """Get jobs data for selected category/level"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')
    fetch_all = request.args.get('fetch_all', 'false').lower() == 'true'

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    if filtered_data.empty:
        return jsonify([])

    # Convert to list of dictionaries for JSON serialization
    jobs_list = []
    for _, row in filtered_data.iterrows():
        job_dict = {
            'auto_score': row['auto_score'],
            'manual_score': row['manual_score']
        }
        # Add level columns if they exist
        for i in range(1, 5):
            level_col = f'level_{i}_name'
            if level_col in row:
                job_dict[level_col] = row[level_col]
        
        jobs_list.append(job_dict)
    
    return jsonify(jobs_list)

@app.route('/api/automation_matrix')
def get_automation_matrix():
    """Get automation matrix data for scatter plot analysis"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    if filtered_data.empty:
        return jsonify({'matrix_data': []})

    # Use vectorized operations for performance
    # Convert to DataFrame operations for speed
    results = []
    
    # Group by level_4_name for vectorized processing
    level_4_groups = filtered_data.groupby('level_4_name')
    
    for level_4_category, group_data in level_4_groups:
        # Extract all tasks for this level_4 category using vectorized operations
        all_tasks_series = group_data['Automatability_Analysis_Parsed'].dropna()
        
        if len(all_tasks_series) == 0:
            continue
            
        # Flatten all tasks into single lists using vectorized operations
        all_tasks = []
        for task_list in all_tasks_series:
            if isinstance(task_list, list):
                all_tasks.extend(task_list)
        
        if not all_tasks:
            continue
            
        # Convert to DataFrame for vectorized operations
        tasks_df = pd.DataFrame(all_tasks)
        
        # Calculate overall automation percentage using vectorized operations
        total_tasks = len(tasks_df)
        if total_tasks == 0:
            continue
            
        automatable_mask = tasks_df['automatability_flag'] == 'Automatable'
        overall_automatable = automatable_mask.sum()
        overall_automation_pct = (overall_automatable / total_tasks) * 100
        
        # Calculate primary task automation percentage using vectorized operations
        primary_mask = tasks_df['importance_classification'].str.lower() == 'primary'
        primary_tasks_df = tasks_df[primary_mask]
        
        if len(primary_tasks_df) == 0:
            primary_automation_pct = 0
        else:
            primary_automatable = (primary_tasks_df['automatability_flag'] == 'Automatable').sum()
            primary_automation_pct = (primary_automatable / len(primary_tasks_df)) * 100
        
        # Determine quadrant and color based on position
        quadrant = ""
        color = ""
        if overall_automation_pct < 50 and primary_automation_pct >= 50:
            quadrant = "upper_left"
            color = "#8B5CF6"  # Purple - Niche/Specialized low risk Vulnerable core
        elif overall_automation_pct >= 50 and primary_automation_pct >= 50:
            quadrant = "upper_right" 
            color = "#EF4444"  # Red - High risk Vulnerable core
        elif overall_automation_pct < 50 and primary_automation_pct < 50:
            quadrant = "lower_left"
            color = "#10B981"  # Green - Safe and stable low risk secure core
        else:  # overall_automation_pct >= 50 and primary_automation_pct < 50
            quadrant = "lower_right"
            color = "#F59E0B"  # Orange - Transformative hot spot high risk secure core
        
        results.append({
            'category': level_4_category,
            'overall_automation_pct': round(overall_automation_pct, 1),
            'primary_automation_pct': round(primary_automation_pct, 1),
            'total_tasks': total_tasks,
            'primary_tasks': len(primary_tasks_df),
            'total_jobs': len(group_data),
            'quadrant': quadrant,
            'color': color
        })
    
    print(f"Generated automation matrix data for {len(results)} level 4 categories")
    return jsonify({'matrix_data': results})

@app.route('/api/task_analysis')
def get_task_analysis():
    """Get comprehensive task analysis for selected category/level"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500

    category = request.args.get('category')
    level = request.args.get('level', '4')

    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]

    # Extract all tasks from the filtered data
    all_tasks = []
    for _, row in filtered_data.iterrows():
        if row['Automatability_Analysis_Parsed']:
            for task in row['Automatability_Analysis_Parsed']:
                all_tasks.append(task)

    if not all_tasks:
        return jsonify({
            'total_tasks': 0,
            'automation_status': {'automatable': 0, 'non_automatable': 0},
            'task_type_distribution': {'primary': 0, 'secondary': 0, 'ancillary': 0},
            'automation_by_type': {},
            'automation_drivers': [],
            'automation_barriers': []
        })

    # 1. Automation Status Overview
    automatable_count = sum(1 for task in all_tasks if task.get('automatability_flag') == 'Automatable')
    non_automatable_count = len(all_tasks) - automatable_count

    # 2. Task Type Distribution
    task_types = [task.get('importance_classification', 'Not specified').lower() for task in all_tasks]
    type_counts = Counter(task_types)
    
    # Normalize task type names
    primary_count = type_counts.get('primary', 0)
    secondary_count = type_counts.get('secondary', 0)
    ancillary_count = type_counts.get('ancillary', 0)
    other_count = sum(count for key, count in type_counts.items() 
                     if key not in ['primary', 'secondary', 'ancillary'])

    # 3. Automation Potential by Task Type
    automation_by_type = {}
    for task_type in ['primary', 'secondary', 'ancillary']:
        type_tasks = [task for task in all_tasks 
                     if task.get('importance_classification', '').lower() == task_type]
        if type_tasks:
            automatable = sum(1 for task in type_tasks if task.get('automatability_flag') == 'Automatable')
            total = len(type_tasks)
            automation_by_type[task_type] = {
                'automatable': automatable,
                'non_automatable': total - automatable,
                'total': total,
                'automation_percentage': round((automatable / total) * 100, 1) if total > 0 else 0
            }

    # 4. Automation Rationale Analysis - OPTIMIZED VERSION
    # Convert all tasks to DataFrame for vectorized operations
    if not all_tasks:
        top_drivers = []
        top_barriers = []
    else:
        # Create DataFrame from all tasks for faster processing
        tasks_df = pd.DataFrame(all_tasks)
        
        # Separate automatable and non-automatable tasks using boolean indexing
        automatable_mask = tasks_df['automatability_flag'] == 'Automatable'
        automatable_df = tasks_df[automatable_mask]
        non_automatable_df = tasks_df[~automatable_mask]
        
        print(f"Found {len(automatable_df)} automatable tasks and {len(non_automatable_df)} non-automatable tasks")
        
        # Fast question extraction using pandas operations
        def extract_questions_fast(df):
            """Extract and count questions from DataFrame using vectorized operations"""
            question_counter = Counter()
            
            # Filter rows that have question field and it's a list
            valid_questions = df['question'].dropna()
            valid_questions = valid_questions[valid_questions.apply(lambda x: isinstance(x, list) and len(x) > 0)]
            
            if len(valid_questions) > 0:
                # Flatten all question lists into a single list using list comprehension
                all_questions = [question for question_list in valid_questions for question in question_list if isinstance(question, str)]
                
                # Clean up questions using list comprehension (faster than loop)
                cleaned_questions = [q.replace('_', ' ').title() for q in all_questions]
                
                # Count all questions at once
                question_counter.update(cleaned_questions)
            
            return question_counter
        
        # Process both datasets in parallel-like fashion
        automation_drivers = extract_questions_fast(automatable_df)
        automation_barriers = extract_questions_fast(non_automatable_df)
        
        print(f"Automation drivers found: {len(automation_drivers)} unique reasons")
        print(f"Automation barriers found: {len(automation_barriers)} unique reasons")
        print(f"Top 5 drivers: {automation_drivers.most_common(5)}")
        print(f"Top 5 barriers: {automation_barriers.most_common(5)}")
        
        # Convert to lists (top 10 each) - this is also fast
        top_drivers = [{'reason': reason, 'count': count} 
                      for reason, count in automation_drivers.most_common(10)]
        top_barriers = [{'reason': reason, 'count': count} 
                       for reason, count in automation_barriers.most_common(10)]

    return jsonify({
        'total_tasks': len(all_tasks),
        'total_jobs': len(filtered_data),
        'automation_status': {
            'automatable': automatable_count,
            'non_automatable': non_automatable_count
        },
        'task_type_distribution': {
            'primary': primary_count,
            'secondary': secondary_count,
            'ancillary': ancillary_count,
            'other': other_count
        },
        'automation_by_type': automation_by_type,
        'automation_drivers': top_drivers,
        'automation_barriers': top_barriers
    })

@app.route('/api/risk_distribution')
def get_risk_distribution():
    """Get risk distribution data for donut chart"""
    if job_data is None and not load_job_data():
        return jsonify({'error': 'No data available'}), 500
    
    category = request.args.get('category')
    level = request.args.get('level', '4')
    
    filtered_data = job_data
    if category:
        level_column = f'level_{level}_name'
        if level_column in job_data.columns:
            filtered_data = job_data[job_data[level_column] == category]
    
    # Calculate risk distribution
    low_risk = len(filtered_data[filtered_data['auto_score'] < 30])
    medium_risk = len(filtered_data[(filtered_data['auto_score'] >= 30) & (filtered_data['auto_score'] < 60)])
    high_risk = len(filtered_data[filtered_data['auto_score'] >= 60])
    
    return jsonify({
        'low_risk': low_risk,
        'medium_risk': medium_risk,
        'high_risk': high_risk,
        'total': len(filtered_data)
    })

if __name__ == '__main__':
    app.run(debug=True)