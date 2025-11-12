# Next Steps to Submit the Fix for Issue #52

## Current Status
✅ Fix is implemented and committed locally on branch `fix/issue-52-ppn-inference`
✅ Tests and documentation are included
❌ Not yet pushed to GitHub (need to fork first)

## Steps to Submit the Pull Request

### Step 1: Create a Fork
1. Go to https://github.com/DragonHPC/dragon
2. Click the "Fork" button in the top-right corner
3. This will create `https://github.com/ssam18/dragon`

### Step 2: Add Fork as Remote and Push
```bash
cd /home/samaresh/src/awesome/dragon
git remote add myfork https://github.com/ssam18/dragon.git
git push myfork fix/issue-52-ppn-inference
```

### Step 3: Create Pull Request
1. Go to https://github.com/ssam18/dragon
2. You should see a banner suggesting to create a PR from the recently pushed branch
3. Click "Compare & pull request"
4. Title: "Fix issue #52: Infer ppn from policies in configure_training_group"
5. Description:
```markdown
## Summary
Fixes #52 - Resolves the documentation vs implementation mismatch in `configure_training_group`

## Problem
The function documented that users could provide either `(ppn + nprocs)` OR `(policies)`, 
but the implementation crashed with `TypeError: unsupported operand type(s) for //: 'int' and 'NoneType'` 
when only policies were provided without ppn.

## Solution
- Automatically infers `ppn` from the policies list when not explicitly provided
- Counts processes per host and uses the maximum as ppn
- Accurately calculates `node_rank` and `local_rank` based on actual policy assignments
- Maintains backward compatibility with explicit ppn+nprocs usage

## Changes
- Modified `src/dragon/native/process_group.py` to add ppn inference logic
- Added accurate rank mapping for policy-based configurations
- Handles both uniform and non-uniform process distributions

## Testing
- Included test script demonstrating the fix works
- Handles uniform distributions (same processes per node)
- Handles non-uniform distributions (different processes per node)

## Commits
- 16394c8: Core fix implementation
- 921db6f: Tests and documentation

Implements Option A as confirmed by @mendygral
```

### Step 4: Link to Issue
Make sure to reference "Fixes #52" in the PR description so GitHub automatically links and closes the issue when merged.

## Alternative: Create PR via GitHub CLI
If you have GitHub CLI installed:
```bash
# First time setup
gh auth login

# Create fork (if not exists)
gh repo fork DragonHPC/dragon --clone=false

# Push to fork
git push https://github.com/ssam18/dragon.git fix/issue-52-ppn-inference

# Create PR
gh pr create --repo DragonHPC/dragon --base main --head ssam18:fix/issue-52-ppn-inference \
  --title "Fix issue #52: Infer ppn from policies in configure_training_group" \
  --body "Fixes #52 - See ISSUE_52_FIX_SUMMARY.md for detailed explanation"
```

## Verification Before Submitting
Run these checks locally:
```bash
# Check that the fix is on the branch
git log --oneline fix/issue-52-ppn-inference | head -5

# View the diff
git diff 6c8463d fix/issue-52-ppn-inference src/dragon/native/process_group.py

# Ensure all files are committed
git status
```

## Files in This Fix
- `src/dragon/native/process_group.py` - Core implementation
- `test_configure_training_group_fix.py` - Test demonstrating fix
- `ISSUE_52_FIX_SUMMARY.md` - Detailed documentation
- `NEXT_STEPS_TO_SUBMIT_FIX.md` - This file

## Notes
- The fix is based on commit 6c8463d where the function existed
- The function may have been removed/modified in later versions
- Confirm with maintainers which branch to target for the PR
