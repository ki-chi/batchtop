use std::io::{self, Stdout, stdout};

use crossterm::{
    cursor::{Hide, Show},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

pub type Tui = Terminal<CrosstermBackend<Stdout>>;

pub struct TerminalGuard {
    terminal: Tui,
}

impl TerminalGuard {
    pub fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut output = stdout();
        if let Err(error) = execute!(output, EnterAlternateScreen, Hide) {
            let _ = disable_raw_mode();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(output)) {
            Ok(mut terminal) => {
                if let Err(error) = terminal.clear() {
                    restore();
                    return Err(error);
                }
                Ok(Self { terminal })
            }
            Err(error) => {
                restore();
                Err(error)
            }
        }
    }

    pub fn terminal_mut(&mut self) -> &mut Tui {
        &mut self.terminal
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.terminal.show_cursor();
        restore();
    }
}

pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        previous(info);
    }));
}

pub fn restore() {
    let _ = disable_raw_mode();
    let _ = execute!(stdout(), Show, LeaveAlternateScreen);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_type_is_constructible_with_test_independent_backend_alias() {
        fn assert_send<T: Send>() {}
        assert_send::<Tui>();
    }
}
